package outlikealambda.traversal;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.walk.Navigator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalComparer {

	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	private static int topicId = 64;

	private static Navigator nav = new Navigator(topicId);

	private static ConnectivityManager basic = ConnectivityManager.unwindAndWalk(topicId);
	private static ConnectivityManager smart = ConnectivityManager.dirtyWalk(topicId);

	@Test
	public void forwardWalkIsDeterministic() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			int size = 30;
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

			List<Integer> personIds = IntStream.range(0, size).boxed().collect(toList());
			Collections.shuffle(personIds);

			final List<Pair<Node, Node>> authorOpinions = IntStream.range(0, opinionCount)
					.mapToObj(i -> Pair.of(getPerson(personIds.get(i)), getOpinion(i)))
					.collect(toList());

			System.out.println("Initial Pass");
			authorOpinions
					.forEach(ao -> basic.setOpinion(ao.getLeft(), ao.getRight()));

			Map<Node, Node> baseConnectionMap = getConnectionMap();

			neo4j.getGraphDatabaseService().getAllRelationships().stream()
					.filter(rel -> rel.isType(Relationships.Types.connected(topicId)))
					.map(rel -> String.format("(%02d)-[%s]->(%02d)", rel.getStartNode().getId(), rel.getType().name(), rel.getEndNode().getId()))
					.forEach(System.out::println);

			assertTrue(0 < baseConnectionMap.entrySet().size());

			for (int passes = 0; passes < shuffleCount; passes++) {
				Collections.shuffle(authorOpinions);
				System.out.println("shuffle #" + (passes + 1));

				clearAuthored();
				clearConnected();
				insertAndCompareConnectionMap(baseConnectionMap, authorOpinions, basic);

				clearAuthored();
				clearConnected();
				insertAndCompareConnectionMap(baseConnectionMap, authorOpinions, smart);
			}

			tx.failure();
		}
	}

	@Test
	public void compareRuntime() {
		int size = 600;
		int opinionCount = 25;
		int shuffleCount = 500;

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

		List<Integer> personIds = IntStream.range(0, size).boxed().collect(toList());
		Collections.shuffle(personIds);

		final List<Pair<Integer, Integer>> authorOpinionIds = IntStream.range(0, opinionCount)
				.mapToObj(i -> Pair.of(personIds.get(i), i))
				.collect(toList());

		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {

			neo4j.getGraphDatabaseService().execute(createStatement);


			List<Pair<Node, Node>> authorOpinions = authorOpinionIds.stream()
					.map(pair -> Pair.of(
							getPerson(pair.getLeft()),
							getOpinion(pair.getRight())
					))
					.collect(Collectors.toList());

			authorOpinions.forEach(ao -> basic.setOpinion(
					ao.getLeft(),
					ao.getRight()
			));

			Map<Node, Node> baseConnectionMap = getConnectionMap();
			assertTrue(0 < baseConnectionMap.entrySet().size());

			long start = System.currentTimeMillis();

			for (int passes = 0; passes < shuffleCount; passes++) {
				clearAuthored();
				clearConnected();

				Collections.shuffle(authorOpinions);

				if ((passes + 1) % 100 == 0) {
					System.out.println("shuffle #" + (passes + 1));
				}

				insertAndCompareConnectionMap(baseConnectionMap, authorOpinions, basic);
			}

			System.out.println("CLEAN:" + (start - System.currentTimeMillis()) + "ms");

			tx.failure();
		}

		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {

			neo4j.getGraphDatabaseService().execute(createStatement);

			List<Pair<Node, Node>> authorOpinions = authorOpinionIds.stream()
					.map(pair -> Pair.of(
							getPerson(pair.getLeft()),
							getOpinion(pair.getRight())
					))
					.collect(Collectors.toList());

			authorOpinions.forEach(ao -> basic.setOpinion(
					ao.getLeft(),
					ao.getRight()
			));

			Map<Node, Node> baseConnectionMap = getConnectionMap();
			assertTrue(0 < baseConnectionMap.entrySet().size());

			long start = System.currentTimeMillis();

			for (int passes = 0; passes < shuffleCount; passes++) {
				clearAuthored();
				clearConnected();

				Collections.shuffle(authorOpinions);

				if ((passes + 1) % 100 == 0) {
					System.out.println("shuffle #" + (passes + 1));
				}

				insertAndCompareConnectionMap(baseConnectionMap, authorOpinions, smart);
			}

			System.out.println("DIRTY:" + (start - System.currentTimeMillis()) + "ms");

			tx.failure();
		}
	}

	private void insertAndCompareConnectionMap(
			Map<Node, Node> baseConnectionMap,
			List<Pair<Node, Node>> authorOpinions,
			ConnectivityManager manager) {

		authorOpinions
				.forEach(ao -> manager.setOpinion(ao.getLeft(), ao.getRight()));

		Map<Node, Node> shuffledInsert = getConnectionMap();

		assertEquals(baseConnectionMap.entrySet().size(), shuffledInsert.entrySet().size());

		shuffledInsert.entrySet().forEach(entry ->
				assertTrue(baseConnectionMap.get(entry.getKey()).equals(entry.getValue()))
		);
	}

	private static void clearConnected() {
		neo4j.getGraphDatabaseService().getAllNodes().stream()
				.filter(nav::isConnected)
				.forEach(nav::clearConnectionState);
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
