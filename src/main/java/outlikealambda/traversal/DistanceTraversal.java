package outlikealambda.traversal;

import example.FullTextIndex;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import outlikealambda.model.Journey;
import outlikealambda.model.Person;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class DistanceTraversal {
	private static final Label personLabel = Label.label("Person");
	private static final Label opinionLabel = Label.label("Opinion");
	private static final Label topicLabel = Label.label("Topic");

	// This field declares that we need a GraphDatabaseService
	// as context when any procedure in this class is invoked
	@Context
	public GraphDatabaseService db;

	// This gives us a log instance that outputs messages to the
	// standard log, normally found under `data/log/console.log`
	@Context
	public Log log;

	@Procedure("test.traverse")
	public void basicTraverse(@Name("id") long personId) {
		db.traversalDescription()
				.breadthFirst()
				.relationships(RelationshipType.withName("TRUSTS"), Direction.OUTGOING)
				.relationships(RelationshipType.withName("TRUSTS_EXPLICITLY"), Direction.OUTGOING)
				.evaluator(Evaluators.toDepth(1))
				.traverse(db.findNode(personLabel, "id", personId))
				.forEach(this::printPath);
	}

	@Procedure("traverse.distance")
	public Stream<Connection> go(
			@Name("userId") long personId,
			@Name("topicId") long topicId,
			@Name("maxDistance") Double maxDistance
	) {

		log.info("Starting traversal for " + personId + " : " + topicId);

		Iterable<Relationship> userConnections = db.findNode(personLabel, "id", personId)
				.getRelationships(Direction.OUTGOING);

		log.info("Got user connections");

		Map<Long, Pair<Node, Relationship>> trusteeRelationships = StreamSupport.stream(userConnections.spliterator(), false)
				.filter(RelationshipLabel.isInteresting(topicId))
				.map(this::trusteeRelationship)
				.collect(toMap(tr -> (long) tr.getLeft().getProperty("id"), Function.identity()));

		log.info(String.format("Got %d trustees", trusteeRelationships.size()));

		Iterable<Relationship> opinionConnections = db.findNode(topicLabel, "id", topicId)
				.getRelationships(Direction.INCOMING, RelationshipType.withName("ADDRESSES"));

		Set<Node> authors = StreamSupport.stream(opinionConnections.spliterator(), false)
				.map(Relationship::getStartNode)
				.map(opinion -> opinion.getRelationships(Direction.INCOMING, RelationshipType.withName("OPINES")))
				.flatMap(opinerConnections -> StreamSupport.stream(opinerConnections.spliterator(), false))
				.map(Relationship::getStartNode)
				.collect(toSet());

		log.info(String.format("Got %d authors", authors.size()));

		PathExpander<Double> expander = new TotalWeightPathExpander(maxDistance, topicId, log);
		Evaluator evaluator = Evaluators.includeWhereEndNodeIs(authors.toArray(new Node[authors.size()]));

		Stream<Path> paths = trusteeRelationships.values().stream()
				.map(trusteeRelationship -> traverse(trusteeRelationship.getLeft(), evaluator, expander, trusteeRelationship.getRight()))
				.flatMap(Traverser::stream);

		Map<Person, List<Journey>> groupedJourneys = trusteeRelationships.values().stream()
				.map(trusteeRelationship -> traverse(trusteeRelationship.getLeft(), evaluator, expander, trusteeRelationship.getRight()))
				.flatMap(Traverser::stream)
				.collect(groupingBy(
						authorFromPath(trusteeRelationships),
						mapping(journeyFromPath(trusteeRelationships), toList())
						)
				);

		log.info(String.format("Got %d journey groups", groupedJourneys.size()));

		return groupedJourneys.entrySet().stream().map(DistanceTraversal::toConnection);
	}

	private Pair<Node, Relationship> trusteeRelationship(Relationship toTrustee) {
		return Pair.of(toTrustee.getEndNode(), toTrustee);
	}

	private Traverser traverse(Node friend, Evaluator evaluator, PathExpander<Double> expander, final Relationship relationship) {
		Double initialCost = RelationshipLabel.getCost(relationship);

		return db.traversalDescription()
				.order(WeightedRelationshipSelectorFactory.create())
				.expand(expander, new InitialBranchState<Double>() {
					@Override public Double initialState(Path path) {
						return initialCost;
					}

					@Override public InitialBranchState<Double> reverse() {
						return this;
					}
				})
				.evaluator(evaluator)
				.traverse(friend);
	}

	private void printPath(Path p) {
		log.info("Path Start");
		printNode(p.startNode());
		p.relationships().forEach(relationship -> {
			log.info(relationship.getType().name());
			printNode(relationship.getEndNode());
		});
		log.info("Path End\n");
	}

	private void printNode(Node n) {
		log.info(labelsToString(n));
		log.info(n.getProperty("name").toString());
	}

	private static String labelsToString(Node n) {
		return StreamSupport.stream(n.getLabels().spliterator(), false)
				.map(Label::name)
				.collect(joining(", "));
	}

	private static Function<Path, Journey> journeyFromPath(Map<Long, Pair<Node, Relationship>> trusteeRelationships) {
		return p -> new Journey(
				fromNode(p.startNode(), trusteeRelationships),
				StreamSupport.stream(p.relationships().spliterator(), false)
						.map(Relationship::getType)
						.map(RelationshipType::name)
						.collect(toList())

		);
	}

	private static Function<Path, Person> authorFromPath(Map<Long, Pair<Node, Relationship>> trusteeRelationships) {
		return p -> fromNode(p.endNode(), trusteeRelationships);
	}

	private static Person fromNode(Node n, Map<Long, Pair<Node, Relationship>> trusteeRelationships) {
		String name = (String) n.getProperty("name");
		long id = (long) n.getProperty("id");
		String relationshipString = Optional.ofNullable(trusteeRelationships.get(id))
				.map(Pair::getRight)
				.map(Relationship::getType)
				.map(RelationshipType::name)
				.orElse("NONE");

		return new Person(name, id, relationshipString);
	}

	public static class Connection {
		// public for serialization
		public final Map<String, Object> author;
		public final List<Map<String, Object>> journeys;

		private Connection(Person author, List<Journey> journeys) {
			this.author = author.toMap();
			this.journeys = journeys.stream()
					.map(Journey::toMap)
					.collect(toList());
		}
	}

	private static Connection toConnection(Map.Entry<Person, List<Journey>> authorGroup) {
		return new Connection(authorGroup.getKey(), authorGroup.getValue());
	}
}
