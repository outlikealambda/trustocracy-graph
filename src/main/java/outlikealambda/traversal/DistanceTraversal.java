package outlikealambda.traversal;

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
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import outlikealambda.model.Connection;
import outlikealambda.model.Influence;
import outlikealambda.model.Journey;
import outlikealambda.model.Person;
import outlikealambda.traversal.expand.TotalWeightPathExpander;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static outlikealambda.traversal.TraversalUtils.goStream;

public class DistanceTraversal {
	private static final Label PERSON_LABEL = Label.label("Person");
	private static final Label TOPIC_LABEL = Label.label("Topic");
	private static final Label OPINION_LABEL = Label.label("Opinion");

	// This field declares that we need a GraphDatabaseService
	// as context when any procedure in this class is invoked
	@Context
	public GraphDatabaseService db;

	// This gives us a log instance that outputs messages to the
	// standard log, normally found under `data/log/console.log`
	@Context
	public Log log;

	@Procedure("influence.person")
	public Stream<Influence> personInfluence(
			@Name("userId") long personId,
			@Name("topicId") long topicId,
			@Name("maxDistance") Double maxDistance
	) {
		log.info("calculating influence for " + personId + topicId);


		return calculatePersonInfluence(personId, topicId, maxDistance);
	}

	@Procedure("influence.opinion")
	public Stream<Influence> opinionInfluence(
			@Name("userId") long opinionId,
			@Name("maxDistance") Double maxDistance
	) {
		Node opinion = db.findNode(OPINION_LABEL, "id", opinionId);

		Node author = opinion
				.getSingleRelationship(RelationshipType.withName("OPINES"), Direction.INCOMING)
				.getStartNode();

		Node topic = opinion
				.getSingleRelationship(RelationshipType.withName("ADDRESSES"), Direction.OUTGOING)
				.getEndNode();

		long authorId = (long) author.getProperty("id");
		long topicId = (long) topic.getProperty("id");

		return calculatePersonInfluence(authorId, topicId, maxDistance);
	}

	private Stream<Influence> calculatePersonInfluence(long personId, long topicId, Double maxDistance) {
		Node user = db.findNode(PERSON_LABEL, "id", personId);

		/**
		 * this is kind of hacky.  I originally pruned branches in the TotalWeightPathExpander by checking
		 * total distance.  Now I _also_ check total distance in the Evaluator, as there are no
		 * end nodes to look for.
		 *
		 * TODO: I wonder if this would be simpler/faster to just do iteratively, since we don't need paths
		 *
		 */
		PathExpander<Double> expander = new TotalWeightPathExpander(maxDistance, topicId, Direction.INCOMING, log);
		PathEvaluator<Double> evaluator = DistanceEvaluators.withinDistance(maxDistance);

		long influence = db.traversalDescription()
				.order(WeightedRelationshipSelectorFactory.create())
				.expand(expander, new InitialBranchState<Double>() {
					@Override public Double initialState(Path path) {
						return 0d;
					}

					@Override public InitialBranchState<Double> reverse() {
						return this;
					}
				})
				.evaluator(evaluator)
				.traverse(user)
				.stream()
				.count();

		return Stream.of(new Influence(personId, topicId, influence));

	}


	@Procedure("traverse.distance")
	public Stream<Connection> findConnected(
			@Name("userId") long personId,
			@Name("topicId") long topicId,
			@Name("maxDistance") Double maxDistance
	) {
		return findOpinions(personId, topicId, maxDistance, false);
	}

	@Procedure("traverse.distance.withUnconnected")
	public Stream<Connection> findAll(
			@Name("userId") long personId,
			@Name("topicId") long topicId,
			@Name("maxDistance") Double maxDistance
	) {
		return findOpinions(personId, topicId, maxDistance, true);

	}

	private Stream<Connection> findOpinions(long personId, long topicId, Double maxDistance, boolean includeUnconnected) {

		log.info("Starting traversal for " + personId + " : " + topicId);

		Iterable<Relationship> userConnections = db.findNode(PERSON_LABEL, "id", personId)
				.getRelationships(Direction.OUTGOING);

		log.info("Got user connections");

		Map<Node, Relationship> trusteeRelationships = goStream(userConnections)
				.filter(RelationshipLabel.isInteresting(topicId))
				.collect(toMap(Relationship::getEndNode, Function.identity()));

		log.info(String.format("Got %d trustees", trusteeRelationships.size()));

		Iterable<Relationship> opinionConnections = db.findNode(TOPIC_LABEL, "id", topicId)
				.getRelationships(Direction.INCOMING, RelationshipType.withName("ADDRESSES"));

		Map<Node, Node> authorOpinions = goStream(opinionConnections)
				.map(Relationship::getStartNode)
				.map(opinion -> opinion.getRelationships(Direction.INCOMING, RelationshipType.withName("OPINES")))
				.flatMap(TraversalUtils::goStream)
				.collect(toMap(Relationship::getStartNode, Relationship::getEndNode));

		log.info(String.format("Got %d authors", authorOpinions.size()));

		PathExpander<Double> expander = new TotalWeightPathExpander(maxDistance, topicId, Direction.OUTGOING, log);
		Evaluator evaluator = Evaluators.includeWhereEndNodeIs(authorOpinions.keySet().toArray(new Node[authorOpinions.size()]));

		Map<Node, List<Path>> connectedPaths = trusteeRelationships.entrySet().stream()
				.map(trusteeRelationship -> traverse(
						trusteeRelationship.getKey(),
						evaluator,
						expander,
						trusteeRelationship.getValue())
				)
				.flatMap(Traverser::stream)
				.collect(groupingBy(Path::endNode));

		Stream<Connection> connected = connectedPaths.entrySet().stream()
				.map(buildWritable(authorOpinions, trusteeRelationships));

		if (includeUnconnected) {
			Stream<Connection> unconnected = authorOpinions.entrySet().stream()
					.filter(ao -> !connectedPaths.containsKey(ao.getKey()))
					.map(ao -> buildUnconnected(personFromNode(ao.getKey()), ao.getValue()));

			return Stream.concat(connected, unconnected);
		} else {
			return connected;
		}
	}

	private static Function<Map.Entry<Node, List<Path>>, Connection> buildWritable(
			Map<Node, Node> authorOpinions,
			Map<Node, Relationship> trusteeRelationships
	) {
		return authorPath -> buildConnection(
				personFromNode(authorPath.getKey(), trusteeRelationships),
				authorOpinions.get(authorPath.getKey()),
				authorPath.getValue().stream()
						.map(journeyFromPath(trusteeRelationships))
						.collect(toList())
		);
	}

	private static Connection buildUnconnected(Person author, Node opinion) {
		return new Connection(
				// no path!
				null,

				// Opinion
				opinion.getAllProperties(),


				author.toMap(),

				// Qualifications
				Optional.ofNullable(opinion.getSingleRelationship(RelationshipType.withName("QUALIFIES"), Direction.INCOMING))
						.map(Relationship::getStartNode)
						.map(Node::getAllProperties)
						.orElse(null)
		);
	}

	private static Connection buildConnection(Person author, Node opinion, List<Journey> journeys) {
		return new Connection(
			// Paths
			journeys.stream()
					.map(Journey::toMap)
					.collect(toList()),
			// Opinion
			opinion.getAllProperties(),


			author.toMap(),

			// Qualifications
			Optional.ofNullable(opinion.getSingleRelationship(RelationshipType.withName("QUALIFIES"), Direction.INCOMING))
					.map(Relationship::getStartNode)
					.map(Node::getAllProperties)
					.orElse(null)
		);
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

	private static Function<Path, Journey> journeyFromPath(Map<Node, Relationship> trusteeRelationships) {
		return p -> new Journey(
				personFromNode(p.startNode(), trusteeRelationships),
				goStream(p.relationships())
						.map(Relationship::getType)
						.map(RelationshipType::name)
						.collect(toList())
		);
	}

	private static Person personFromNode(Node n, Map<Node, Relationship> trusteeRelationships) {
		String name = (String) n.getProperty("name");
		long id = (long) n.getProperty("id");

		String relationshipString = Optional.ofNullable(trusteeRelationships.get(n))
				.map(Relationship::getType)
				.map(RelationshipType::name)
				.orElse("NONE");

		return new Person(name, id, relationshipString);
	}

	private static Person personFromNode(Node n) {
		String name = (String) n.getProperty("name");
		long id = (long) n.getProperty("id");

		return new Person(name, id, "NONE");
	}
}
