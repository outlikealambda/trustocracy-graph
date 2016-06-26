package example;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import outlikealambda.api.Journey;
import outlikealambda.api.Person;
import outlikealambda.traversal.RelationshipLabel;
import outlikealambda.traversal.TotalWeightPathExpander;
import outlikealambda.traversal.WeightedRelationshipSelectorFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * This is an example showing how you could expose Neo4j's full text indexes as
 * two procedures - one for updating indexes, and one for querying by label and
 * the lucene query language.
 */
public class FullTextIndex
{
    // Only static fields and @Context-annotated fields are allowed in
    // Procedure classes. This static field is the configuration we use
    // to create full-text indexes.
    private static final Map<String,String> FULL_TEXT =
            stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" );

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * This declares the first of two procedures in this class - a
     * procedure that performs queries in a legacy index.
     *
     * It returns a Stream of Records, where records are
     * specified per procedure. This particular procedure returns
     * a stream of {@link SearchHit} records.
     *
     * The arguments to this procedure are annotated with the
     * {@link Name} annotation and define the position, name
     * and type of arguments required to invoke this procedure.
     * There is a limited set of types you can use for arguments,
     * these are as follows:
     *
     * <ul>
     *     <li>{@link String}</li>
     *     <li>{@link Long} or {@code long}</li>
     *     <li>{@link Double} or {@code double}</li>
     *     <li>{@link Number}</li>
     *     <li>{@link Boolean} or {@code boolean}</li>
     *     <li>{@link java.util.Map} with key {@link String} and value {@link Object}</li>
     *     <li>{@link java.util.List} of elements of any valid argument type, including {@link java.util.List}</li>
     *     <li>{@link Object}, meaning any of the valid argument types</li>
     * </ul>
     *
     * @param label the label name to query by
     * @param query the lucene query, for instance `name:Brook*` to
     *              search by property `name` and find any value starting
     *              with `Brook`. Please refer to the Lucene Query Parser
     *              documentation for full available syntax.
     * @return the nodes found by the query
     */
    @Procedure("example.search")
    @PerformsWrites // TODO: This is here as a workaround, because index().forNodes() is not read-only
    public Stream<SearchHit> search( @Name("label") String label,
                                     @Name("query") String query )
    {
        String index = indexName( label );

        db.schema().getIndexes().forEach(idx -> log.info(idx.getLabel().name()));
        String [] indices = db.index().nodeIndexNames();

        for (String idx : indices) {
          log.info(idx);
        }

        // Avoid creating the index, if it's not there we won't be
        // finding anything anyway!
        if( !db.index().existsForNodes( index ))
        {
            // Just to show how you'd do logging
            log.info( "Skipping index query since index does not exist: `%s`", index );
            return Stream.empty();
        }

        // If there is an index, do a lookup and convert the result
        // to our output record.
        return db.index()
                .forNodes( index )
                .query( query )
                .stream()
                .map( SearchHit::new );
    }

    /**
     * This is the second procedure defined in this class, it is used to update the
     * index with nodes that should be queryable. You can send the same node multiple
     * times, if it already exists in the index the index will be updated to match
     * the current state of the node.
     *
     * This procedure works largely the same as {@link #search(String, String)},
     * with two notable differences. One, it is annotated with {@link PerformsWrites},
     * which is <i>required</i> if you want to perform updates to the graph in your
     * procedure.
     *
     * Two, it returns {@code void} rather than a stream. This is simply a short-hand
     * for saying our procedure always returns an empty stream of empty records.
     *
     * @param nodeId the id of the node to index
     * @param propKeys a list of property keys to index, only the ones the node
     *                 actually contains will be added
     */
    @Procedure("example.index")
    @PerformsWrites
    public void index( @Name("nodeId") long nodeId,
                       @Name("properties") List<String> propKeys )
    {
        Node node = db.getNodeById( nodeId );

        // Load all properties for the node once and in bulk,
        // the resulting set will only contain those properties in `propKeys`
        // that the node actually contains.
        Set<Map.Entry<String,Object>> properties =
                node.getProperties( propKeys.toArray( new String[0] ) ).entrySet();

        // Index every label (this is just as an example, we could filter which labels to index)
        for ( Label label : node.getLabels() )
        {
            Index<Node> index = db.index().forNodes( indexName( label.name() ), FULL_TEXT );

            // In case the node is indexed before, remove all occurrences of it so
            // we don't get old or duplicated data
            index.remove( node );

            // And then index all the properties
            for ( Map.Entry<String,Object> property : properties )
            {
                index.add( node, property.getKey(), property.getValue() );
            }
        }
    }

    private static final Label personLabel = Label.label("Person");
	private static final Label opinionLabel = Label.label("Opinion");
	private static final Label topicLabel = Label.label("Topic");

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

	@Procedure("test.distance")
	public Stream<Connection> go(	@Name("userId") long personId,
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

		return groupedJourneys.entrySet().stream().map(FullTextIndex::toConnection);
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
				.collect(Collectors.joining(", "));
	}
    /**
     * This is the output record for our search procedure. All procedures
     * that return results return them as a Stream of Records, where the
     * records are defined like this one - customized to fit what the procedure
     * is returning.
     *
     * These classes can only have public non-final fields, and the fields must
     * be one of the following types:
     *
     * <ul>
     *     <li>{@link String}</li>
     *     <li>{@link Long} or {@code long}</li>
     *     <li>{@link Double} or {@code double}</li>
     *     <li>{@link Number}</li>
     *     <li>{@link Boolean} or {@code boolean}</li>
     *     <li>{@link org.neo4j.graphdb.Node}</li>
     *     <li>{@link org.neo4j.graphdb.Relationship}</li>
     *     <li>{@link org.neo4j.graphdb.Path}</li>
     *     <li>{@link java.util.Map} with key {@link String} and value {@link Object}</li>
     *     <li>{@link java.util.List} of elements of any valid field type, including {@link java.util.List}</li>
     *     <li>{@link Object}, meaning any of the valid field types</li>
     * </ul>
     */
    public static class SearchHit
    {
        // This records contain a single field named 'nodeId'
        public long nodeId;

        public SearchHit( Node node )
        {
            this.nodeId = node.getId();
        }
    }

    private String indexName( String label )
    {
        return label;
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

	private static class Connection {
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

