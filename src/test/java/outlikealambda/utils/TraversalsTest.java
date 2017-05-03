package outlikealambda.utils;

import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.TestUtils;
import outlikealambda.traversal.walk.Navigator;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class TraversalsTest {
	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	private static Function<Integer, Node> getPerson = TestUtils.getPerson(neo4j);

	private static int topicId = 64;

	private static Navigator nav = new Navigator(topicId);

	@Test
	public void followAndMeasure() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String e = "e"; // ranked, but not connected
			String o = "opinion";

			String create = TestUtils.createWalkable(topicId)
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addPerson(e, 5)
					.addOpinion(o, 0)
					.connectAuthored(a, o)
					.connectConnected(d, b)
					.connectConnected(c, b)
					.connectConnected(b, a)
					.connectConnected(a, o)
					.connectRanked(e, d, 1)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = getPerson.apply(1);
			Node bNode = getPerson.apply(2);
			Node cNode = getPerson.apply(3);
			Node dNode = getPerson.apply(4);

			assertEquals(aNode, Traversals.follow(nav, aNode));
			assertEquals(aNode, Traversals.follow(nav, bNode));
			assertEquals(aNode, Traversals.follow(nav, cNode));
			assertEquals(aNode, Traversals.follow(nav, dNode));

			assertEquals(4, Traversals.measureInfluence(nav, aNode));
			assertEquals(3, Traversals.measureInfluence(nav, bNode));
			assertEquals(1, Traversals.measureInfluence(nav, cNode));
			assertEquals(1, Traversals.measureInfluence(nav, dNode));

			tx.failure();
		}
	}
}
