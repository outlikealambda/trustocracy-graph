package outlikealambda.traversal.walk;

import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.TestUtils;
import outlikealambda.utils.Traversals;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DirtyBlazerTest {
	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	private static int topicId = 64;

	private static Navigator nav = new Navigator(topicId);

	private static DirtyBlazer fixture = new DirtyBlazer(nav);


	@Test
	public void blazeBasic() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			// while all nodes are ranked to this one, none should follow
			// that path, because it's not connected
			String disjoint = "disjoint";
			String o = "opinion";

			String create = TestUtils.createWalkable(topicId)
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addPerson(disjoint, 5)
					.addOpinion(o, 0)
					.connectAuthored(a, o)
					.connectRanked(b, a, 1)
					.connectManual(c, b)
					.connectRanked(d, c, 1)
					.connectRanked(a, disjoint, 0)
					.connectRanked(b, disjoint, 0)
					.connectRanked(c, disjoint, 0)
					.connectRanked(d, disjoint, 0)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = getPerson(1);
			Node bNode = getPerson(2);
			Node cNode = getPerson(3);
			Node dNode = getPerson(4);
			Node disjointNode = getPerson(5);

			fixture.go(dNode);

			assertEquals(aNode, Traversals.follow(nav, aNode));
			assertEquals(aNode, Traversals.follow(nav, bNode));
			// should follow a manual connection
			assertEquals(aNode, Traversals.follow(nav, cNode));
			assertEquals(aNode, Traversals.follow(nav, dNode));

			assertTrue(nav.isDisjoint(disjointNode));

			tx.failure();
		}
	}

	@Test
	public void nodesAvoidActiveCycles() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String z = "z";
			String o = "opinion";

			String create = TestUtils.createWalkable(topicId)
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addPerson(z, 5)
					.addOpinion(o, 0)
					.connectAuthored(z, o)
					.connectRanked(b, a, 0)
					.connectRanked(c, b, 0)
					.connectRanked(d, c, 0)
					.connectRanked(d, z, 1)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = getPerson(1);
			Node bNode = getPerson(2);
			Node cNode = getPerson(3);
			Node dNode = getPerson(4);
			Node zNode = getPerson(5);

			// Should attempt to go through c, which should cycle and mark all nodes
			// as disjoint.
			// Should then attempt z, which should succeed
			fixture.go(dNode);

			assertEquals(zNode, Traversals.follow(nav, dNode));
			assertEquals(zNode, Traversals.follow(nav, zNode));

			assertTrue(nav.isDisjoint(aNode));
			assertTrue(nav.isDisjoint(bNode));
			assertTrue(nav.isDisjoint(cNode));

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

			Node klbNode = getPerson(1);
			Node mbNode = getPerson(2);
			Node ngNode = getPerson(3);
			Node srNode = getPerson(4);
			Node llNode = getPerson(5);

			fixture.go(klbNode);

			assertEquals(klbNode, Traversals.follow(nav, klbNode));
			assertEquals(klbNode, Traversals.follow(nav, srNode));
			assertEquals(klbNode, Traversals.follow(nav, mbNode));

			assertTrue(nav.isDisjoint(llNode));
			assertTrue(nav.isDisjoint(ngNode));

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

			Node wsNode = getPerson(1);
			Node cdNode = getPerson(2);
			Node slNode = getPerson(3);
			Node gfNode = getPerson(4);
			Node crNode = getPerson(5);

			// Should attempt to go through c, which should cycle and mark all nodes
			// as disjoint.
			// Should then attempt z, which should succeed
			Arrays.asList(
					wsNode,
					cdNode,
					slNode,
					gfNode,
					crNode
			).forEach(fixture::go);

			assertEquals(wsNode, Traversals.follow(nav, wsNode));
			assertEquals(wsNode, Traversals.follow(nav, cdNode));
			assertEquals(wsNode, Traversals.follow(nav, slNode));
			assertEquals(wsNode, Traversals.follow(nav, gfNode));
			assertEquals(wsNode, Traversals.follow(nav, crNode));

			tx.failure();
		}
	}

	private Node getPerson(int id) {
		return neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", id);
	}

	private Node getOpinion(int id) {
		return neo4j.getGraphDatabaseService().findNode(Label.label("Opinion"), "id", id);
	}
}
