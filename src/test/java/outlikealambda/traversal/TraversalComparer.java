package outlikealambda.traversal;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.unwind.BasicUnwinder;
import outlikealambda.traversal.walk.Navigator;
import outlikealambda.traversal.walk.Walker;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalComparer {

	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	private static int topicId = 64;

	private static Navigator nav = new Navigator(topicId);

	private static ConnectivityManager fixture = new ConnectivityManager(
			nav,
			new Walker(nav),
			new BasicUnwinder(nav)
	);

	@Test
	public void forwardWalkIsDeterministic() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			int size = 50;
			int opinionCount = 10;
			int shuffleCount = 100;


			boolean[][] matrix = RelationshipMatrix.build(size);

			TestUtils.Walkable builder = new TestUtils.Walkable(0);

			// initialize persons
			IntStream.range(0, size)
					.forEach(builder::addPersonIdOnly);

			// initialize opinions
			IntStream.range(0, opinionCount)
					.forEach(builder::addOpinionIdOnly);

			// initialize ranked relationships
			RelationshipMatrix.toDirectedTriples(matrix)
					.forEach(triple -> builder.connectRankedById(triple.getLeft(), triple.getRight(), triple.getMiddle()));

			String createStatement = builder.build();

			System.out.println(createStatement.replace(",", ",\n"));

			neo4j.getGraphDatabaseService().execute(createStatement);

			Random rand = new Random();
			final List<Pair<Node, Node>> authorOpinions = IntStream.range(0, opinionCount)
					.mapToObj(i -> Pair.of(getPerson(rand.nextInt(size)), getOpinion(i)))
					.collect(Collectors.toList());

			System.out.println("Initial Pass");
			authorOpinions
					.forEach(ao -> fixture.setOpinion(ao.getLeft(), ao.getRight()));

			Map<Node, Node> baseConnectionMap = getConnectionMap();

			neo4j.getGraphDatabaseService().getAllRelationships().stream()
					.filter(rel -> rel.isType(RelationshipTypes.connected(topicId)))
					.map(rel -> String.format("(%02d)-[%s]->(%02d)", rel.getStartNode().getId(), rel.getType().name(), rel.getEndNode().getId()))
					.forEach(System.out::println);

			assertTrue(0 < baseConnectionMap.entrySet().size());

			for (int passes = 0; passes < shuffleCount; passes++) {
				clearAuthored();
				clearConnected();

				Collections.shuffle(authorOpinions);

				System.out.println("shuffle #" + (passes + 1));

				authorOpinions
						.forEach(ao -> fixture.setOpinion(ao.getLeft(), ao.getRight()));

				Map<Node, Node> shuffledInsert = getConnectionMap();

				assertEquals(baseConnectionMap.entrySet().size(), shuffledInsert.entrySet().size());

				shuffledInsert.entrySet().forEach(entry ->
						assertTrue(baseConnectionMap.get(entry.getKey()).equals(entry.getValue()))
				);
			}

			tx.failure();
		}
	}

	private static void clearConnected() {
		neo4j.getGraphDatabaseService().getAllNodes().stream()
				.filter(nav::isConnected)
				.forEach(source -> nav.setTarget(source, null));
	}

	private static void clearAuthored() {
		neo4j.getGraphDatabaseService().getAllNodes().stream()
				.filter(nav::isAuthor)
				.forEach(author -> nav.setOpinion(author, null));
	}

	private static Map<Node, Node> getConnectionMap() {
		return neo4j.getGraphDatabaseService().getAllNodes().stream()
				.filter(nav::isConnected)
				.map(nav::getConnectionOut)
				.collect(toMap(
						Relationship::getStartNode,
						Relationship::getEndNode
				));
	}

	private Node getPerson(int id) {
		return neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", id);
	}

	private Node getOpinion(int id) {
		return neo4j.getGraphDatabaseService().findNode(Label.label("Opinion"), "id", id);
	}
}
