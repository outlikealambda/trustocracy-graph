package outlikealambda.traversal.utils;

import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.Nodes;
import outlikealambda.traversal.TestUtils;
import outlikealambda.traversal.walk.Navigator;
import outlikealambda.utils.Traversals;

import static org.junit.Assert.assertEquals;

public class TraversalsTest {
	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

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

			Node aNode = getPerson(1);
			Node bNode = getPerson(2);
			Node cNode = getPerson(3);
			Node dNode = getPerson(4);

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

	private Node getPerson(long id) {
		return neo4j.getGraphDatabaseService().findNode(Nodes.Labels.PERSON, Nodes.Fields.ID, id);
	}
}
