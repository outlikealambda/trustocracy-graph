package outlikealambda.traversal.walk;

import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.TestUtils;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WalkerTest {

	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	private static int topicId = 64;

	private static Navigator nav = new Navigator(topicId);

	private static Walker fixture = new Walker(nav);

	@Test
	public void unwindClearsSubtreeOnly() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String e = "e";

			String create = TestUtils.createWalkable(topicId)
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addPerson(e, 5)
					.connectRanked(b, a, 0)
					.connectRanked(c, b, 0)
					.connectRanked(d, b, 0)
					.connectRanked(e, d, 0)
					.connectConnected(b, a)
					.connectConnected(c, b)
					.connectConnected(d, b)
					.connectConnected(e, d)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = getPerson(1);
			Node bNode = getPerson(2);
			Node cNode = getPerson(3);
			Node dNode = getPerson(4);
			Node eNode = getPerson(5);

			Set<Node> cleared = fixture.collectClearedUpstream(dNode);

			// a <- b <- c should still be connected
			assertEquals(aNode, nav.getConnectionOut(bNode).getEndNode());
			assertEquals(bNode, nav.getConnectionOut(cNode).getEndNode());

			// d and e should not be connected
			assertFalse(nav.isConnected(dNode));
			assertFalse(nav.isConnected(eNode));

			// d and e should be in the cleared set
			assertEquals(2, cleared.size());
			assertTrue(cleared.contains(dNode));
			assertTrue(cleared.contains(eNode));

			tx.failure();
		}
	}

	@Test
	public void unwindClearsRankedCycle() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String b = "b";
			String c = "c";
			String d = "d";

			String create = TestUtils.createWalkable(topicId)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.connectRanked(c, b, 0)
					.connectRanked(d, c, 0)
					.connectRanked(b, d, 1)
					.connectConnected(c, b)
					.connectConnected(d, c)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node bNode = getPerson(2);
			Node cNode = getPerson(3);
			Node dNode = getPerson(4);

			Set<Node> cleared = fixture.collectClearedUpstream(dNode);

			// b, c and d should not be connected
			assertFalse(nav.isConnected(bNode));
			assertFalse(nav.isConnected(cNode));
			assertFalse(nav.isConnected(dNode));

			// b, c and d should be in the cleared set
			assertEquals(3, cleared.size());
			assertTrue(cleared.contains(bNode));
			assertTrue(cleared.contains(cNode));
			assertTrue(cleared.contains(dNode));

			tx.failure();
		}
	}

	private Node getPerson(int id) {
		return neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", id);
	}
}
