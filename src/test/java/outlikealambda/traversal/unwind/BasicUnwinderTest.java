package outlikealambda.traversal.unwind;

import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.TestUtils;
import outlikealambda.traversal.walk.Navigator;

import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BasicUnwinderTest {
	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	private static Function<Integer, Node> getPerson = TestUtils.getPerson(neo4j);

	private static int topicId = 64;

	private static Navigator nav = new Navigator(topicId);

	private static Unwinder fixture = new BasicUnwinder(nav);

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

			Node aNode = getPerson.apply(1);
			Node bNode = getPerson.apply(2);
			Node cNode = getPerson.apply(3);
			Node dNode = getPerson.apply(4);
			Node eNode = getPerson.apply(5);

			Set<Node> cleared = fixture.unwind(dNode);

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

			Node bNode = getPerson.apply(2);
			Node cNode = getPerson.apply(3);
			Node dNode = getPerson.apply(4);

			Set<Node> cleared = fixture.unwind(dNode);

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

	@Test
	public void unwindAddsUnconnectedNodes() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";

			String create = TestUtils.createWalkable(topicId)
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.connectRanked(b, a, 0)
					.connectRanked(c, b, 0)
					.connectRanked(d, c, 0)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = getPerson.apply(1);
			Node bNode = getPerson.apply(2);
			Node cNode = getPerson.apply(3);
			Node dNode = getPerson.apply(4);

			Set<Node> cleared = fixture.unwind(aNode);

			assertTrue(cleared.contains(aNode));
			assertTrue(cleared.contains(bNode));
			assertTrue(cleared.contains(cNode));
			assertTrue(cleared.contains(dNode));

			tx.failure();
		}
	}

	@Test
	public void unwindAndBlaze1() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String klb = "klb";
			String mb = "mb";
			String ng = "ng";
			String sr = "sr";
			String ll = "ll";
			String o = "opinion";

			String create = TestUtils.createWalkable(topicId)
					.addPerson(klb, 1)
					.addPerson(mb, 2)
					.addPerson(ng, 3)
					.addPerson(sr, 4)
					.addPerson(ll, 5)
					.addOpinion(o, 0)
					.connectAuthored(klb, o)
					.connectRanked(klb, sr, 0)
					.connectRanked(klb, ll, 1)
					.connectRanked(mb, sr, 0)
					.connectRanked(mb, ng, 1)
					.connectRanked(mb, ll, 2)
					.connectRanked(mb, klb, 3)
					.connectRanked(ng, ll, 0)
					.connectRanked(ng, sr, 1)
					.connectRanked(sr, ng, 0)
					.connectRanked(sr, klb, 1)
					.connectRanked(sr, ll, 2)
					.connectRanked(ll, ng, 0)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node klbNode = getPerson.apply(1);
			Node mbNode = getPerson.apply(2);
			Node ngNode = getPerson.apply(3);
			Node srNode = getPerson.apply(4);
			Node llNode = getPerson.apply(5);

			// Should attempt to go through c, which should cycle and mark all nodes
			// as disjoint.
			// Should then attempt z, which should succeed
			Set<Node> unwound = fixture.unwind(klbNode);

			assertTrue(unwound.contains(klbNode));
			assertTrue(unwound.contains(mbNode));
			assertTrue(unwound.contains(ngNode));
			assertTrue(unwound.contains(srNode));
			assertTrue(unwound.contains(llNode));

			tx.failure();
		}
	}

	/**
	 * this is non-deterministic with the ConnectivityAdjuster
	 */
	@Test
	public void unwindAndBlaze2() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String ws = "ws";
			String cd = "cd";
			String sl = "sl";
			String gf = "gf";
			String cr = "cr";
			String o = "opinion";

			String create = TestUtils.createWalkable(topicId)
					.addPerson(ws, 1)
					.addPerson(cd, 2)
					.addPerson(sl, 3)
					.addPerson(gf, 4)
					.addPerson(cr, 5)
					.addOpinion(o, 0)
					.connectAuthored(ws, o)
					.connectRanked(ws, cr, 0)
					.connectRanked(ws, sl, 1)
					.connectRanked(cd, sl, 0)
					.connectRanked(cd, ws, 1)
					.connectRanked(sl, ws, 0)
					.connectRanked(sl, cd, 1)
					.connectRanked(gf, sl, 0)
					.connectRanked(gf, cr, 1)
					.connectRanked(cr, ws, 0)
					.connectRanked(cr, sl, 1)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node wsNode = getPerson.apply(1);
			Node cdNode = getPerson.apply(2);
			Node slNode = getPerson.apply(3);
			Node gfNode = getPerson.apply(4);
			Node crNode = getPerson.apply(5);

			// Should attempt to go through c, which should cycle and mark all nodes
			// as disjoint.
			// Should then attempt z, which should succeed
			Set<Node> unwound = fixture.unwind(wsNode);

			assertTrue(unwound.contains(wsNode));
			assertTrue(unwound.contains(cdNode));
			assertTrue(unwound.contains(slNode));
			assertTrue(unwound.contains(gfNode));
			assertTrue(unwound.contains(crNode));

			tx.failure();
		}
	}
}
