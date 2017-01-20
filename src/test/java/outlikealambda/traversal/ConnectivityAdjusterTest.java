package outlikealambda.traversal;

import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectivityAdjusterTest {

	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	private static RelationshipFilter rf = new RelationshipFilter(1);

	private static ConnectivityAdjuster adjuster = new ConnectivityAdjuster(rf);

	@Test
	public void getDesignatedAuthor() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String z = "z";
			String opinion = "opinion";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addPerson(z, 5)
					.addOpinion(opinion, 1)
					.connectRanked(a, b, 1)
					.connectRanked(b, d, 1)
					.connectRanked(z, a, 1)
					.connectProvisional(a, c, rf)
					.connectProvisional(b, c, rf)
					.connectProvisional(c, d, rf)
					.connectAuthored(d, opinion, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node dNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);
			Node zNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 5);

			// should not count z-RANKED->a as influence
			assertEquals(dNode, adjuster.getDesignatedAuthor(aNode).get());
			assertEquals(dNode, adjuster.getDesignatedAuthor(bNode).get());
			assertEquals(dNode, adjuster.getDesignatedAuthor(cNode).get());
			assertEquals(dNode, adjuster.getDesignatedAuthor(dNode).get());
			assertFalse(adjuster.getDesignatedAuthor(zNode).isPresent());

			tx.failure();
		}
	}

	@Test
	public void calculateInfluence() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String z = "z";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addPerson(z, 5)
					.connectRanked(a, b, 1)
					.connectRanked(b, d, 1)
					.connectRanked(z, a, 1)
					.connectProvisional(a, c, rf)
					.connectProvisional(b, c, rf)
					.connectProvisional(c, d, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node dNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);
			Node zNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 5);

			// should not count z-RANKED->a as influence
			assertEquals(1, adjuster.calculateInfluence(aNode));
			assertEquals(1, adjuster.calculateInfluence(bNode));
			assertEquals(3, adjuster.calculateInfluence(cNode));
			assertEquals(4, adjuster.calculateInfluence(dNode));
			assertEquals(1, adjuster.calculateInfluence(zNode));

			tx.failure();
		}
	}


	@Test
	public void targetIsUnconnectedRemovesProvisionals() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String o = "opinion";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addOpinion(o, 1)
					.connectRanked(a, b, 1)
					.connectRanked(b, d, 1)
					.connectAuthored(d, o, rf)
					.connectProvisional(a, b, rf)
					.connectProvisional(b, d, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			// creating b-MANUAL->c should delete b-PROVISIONAL->c and a-PROVISIONAL->b
			adjuster.setTarget(bNode, cNode);

			// the cycle should cause all provisional relationships to clear
			assertFalse(rf.getTargetedOutgoing(aNode).isPresent());
			assertFalse(bNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));
			assertTrue(bNode.hasRelationship(Direction.OUTGOING, rf.getManualType()));
			assertTrue(cNode.hasRelationship(Direction.INCOMING, rf.getManualType()));

			tx.failure();
		}
	}

	@Test
	public void targetSelectionCreatesCycle() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String o = "opinion";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addOpinion(o, 1)
					.connectRanked(a, b, 1)
					.connectRanked(b, c, 1)
					.connectRanked(c, a, 1)
					.connectManual(a, c, rf)
					.connectAuthored(d, o, rf)
					.connectProvisional(b, c, rf)
					.connectProvisional(c, a, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node dNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);

			// deleting a-MANUAL->d should connect a-PROVISIONAL->b and create a cycle
			adjuster.clearTarget(aNode);

			// the cycle should cause all provisional relationships to clear
			assertFalse(rf.getTargetedOutgoing(aNode).isPresent());
			assertFalse(rf.getTargetedOutgoing(bNode).isPresent());
			assertFalse(rf.getTargetedOutgoing(cNode).isPresent());

			tx.failure();
		}
	}

	@Test
	public void removeManualConnectedTargetToGetProvisional() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String o = "opinion";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addOpinion(o, 1)
					.connectRanked(a, c, 1)
					.connectRanked(d, a, 1)
					.connectManual(a, b, rf)
					.connectAuthored(c, o, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node dNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);

			// deleting a-MANUAL->b should connect a-PROVISIONAL->c and d-PROVISIONAL->a
			adjuster.clearTarget(aNode);

			// b should still manually point to a
			assertEquals(cNode, rf.getTargetedOutgoing(aNode).get().getEndNode());
			assertEquals(aNode, rf.getTargetedOutgoing(dNode).get().getEndNode());
			assertTrue(aNode.hasRelationship(rf.getProvisionalType(), Direction.OUTGOING));
			assertTrue(cNode.hasRelationship(rf.getProvisionalType(), Direction.INCOMING));
			assertTrue(dNode.hasRelationship(rf.getProvisionalType(), Direction.OUTGOING));
			assertTrue(aNode.hasRelationship(rf.getProvisionalType(), Direction.INCOMING));

			tx.failure();
		}
	}

	@Test
	public void addManualTarget() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String o = "opinion";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addOpinion(o, 1)
					.connectRanked(a, b, 1)
					.connectAuthored(c, o, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			// connecting b-MANUAL->c should connect a-PROVISIONAL->b
			adjuster.setTarget(bNode, cNode);

			// b should still manually point to a
			assertEquals(cNode, rf.getTargetedOutgoing(bNode).get().getEndNode());
			assertTrue(aNode.hasRelationship(rf.getProvisionalType(), Direction.OUTGOING));
			assertTrue(bNode.hasRelationship(rf.getProvisionalType(), Direction.INCOMING));

			tx.failure();
		}

	}

	@Test
	public void switchManualTargets() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectManual(a, b, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			adjuster.setTarget(aNode, cNode);

			// b should still manually point to a
			assertEquals(cNode, rf.getTargetedOutgoing(aNode).get().getEndNode());

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionShouldFlipChildrenOfManual() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectManual(b, a, rf)
					.connectRanked(c, b, 1)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipGainedConnection(r));

			// b should still manually point to a
			assertEquals(startNode, bNode.getSingleRelationship(rf.getManualType(), Direction.OUTGOING).getEndNode());

			// c should now provisionally point to b
			assertEquals(bNode, cNode.getSingleRelationship(rf.getProvisionalType(), Direction.OUTGOING).getEndNode());

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionShouldNotProvisionRankedIncomingWithOtherManual() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectRanked(b, a, 1)
					.connectManual(b, c, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipGainedConnection(r));

			// b should still manually point to c
			assertEquals(cNode, bNode.getSingleRelationship(rf.getManualType(), Direction.OUTGOING).getEndNode());

			// b should still have a ranked connection to a
			assertEquals(startNode, bNode.getSingleRelationship(rf.getRankedType(), Direction.OUTGOING).getEndNode());

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionShouldNotProvisionRankedIncomingWithHigherProvisional() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectRanked(b, a, 2)
					.connectRanked(b, c, 1)
					.connectProvisional(b, c, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipGainedConnection(r));

			// b should still provisionally point to c
			assertEquals(cNode, bNode.getSingleRelationship(rf.getProvisionalType(), Direction.OUTGOING).getEndNode());

			// incoming to startNode should still be ranked from b
			assertEquals(bNode, startNode.getSingleRelationship(rf.getRankedType(), Direction.INCOMING).getStartNode());

			assertFalse(startNode.hasRelationship(rf.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionShouldProvisionRankedIncomingWithLowerProvisional() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			// a needs to pass the isConnected check, so point it to an opinion
			// the same for c
			String opinion = "opinion";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectRanked(b, a, 1)
					.connectRanked(b, c, 2)
					.connectProvisional(b, c, rf)
					.connectAuthored(a, opinion, rf)
					.connectAuthored(c, opinion, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipGainedConnection(r));

			// b should now provisionally point to a
			assertEquals(startNode, bNode.getSingleRelationship(rf.getProvisionalType(), Direction.OUTGOING).getEndNode());

			// incoming to cNode should be ranked from b
			assertEquals(bNode, cNode.getSingleRelationship(rf.getRankedType(), Direction.INCOMING).getStartNode());

			assertFalse(cNode.hasRelationship(rf.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionCreatesCycle() {
		// edge case, but possible
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String e = "e";

			// c needs to pass the isConnected check, so point it to an opinion
			String opinion = "opinion";

			/*
			 * This is the starting state of the graph.  What kicks it off is
			 * b switching from manual->e to manual->c.  This causes a-provisional->b,
			 * which causes c-provisional->a.
			 *
			 * This should trigger a cycle and the deletion of all provisional relationships
			 *
			 */
			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addPerson(e, 5)
					.connectRanked(a, b, 1)
					.connectRanked(b, c, 1)
					.connectRanked(c, a, 1)
					.connectRanked(c, d, 2)
					.connectManual(b, e, rf)
					.connectProvisional(c, d, rf)
					.connectAuthored(d, opinion, rf)
					.build();

			// starting state
			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			// delete starting b-manual->e connection
			bNode.getSingleRelationship(rf.getManualType(), Direction.OUTGOING).delete();

			// create b-manual->c connection
			bNode.createRelationshipTo(cNode, rf.getManualType());

			// this should trigger the a-flip, then the c-flip, and then... cycle!
			bNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipGainedConnection(r));

			// b should manually point to c
			assertEquals(cNode, bNode.getSingleRelationship(rf.getManualType(), Direction.OUTGOING).getEndNode());

			// provisionals should be gone from c->a, a->b
			assertFalse(cNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));
			assertFalse(aNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionShouldProvisionRankedIncomingWithNoPreviousProvisional() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			// a needs to pass the isConnected check, so point it to an opinion
			// the same for c
			String opinion = "opinion";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectRanked(b, a, 2)
					.connectRanked(b, c, 1)
					.connectAuthored(a, opinion, rf)
					.connectAuthored(c, opinion, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipGainedConnection(r));

			// b should now provisionally point to a
			assertEquals(startNode, bNode.getSingleRelationship(rf.getProvisionalType(), Direction.OUTGOING).getEndNode());

			// incoming to cNode should be ranked from b
			assertEquals(bNode, cNode.getSingleRelationship(rf.getRankedType(), Direction.INCOMING).getStartNode());

			assertFalse(cNode.hasRelationship(rf.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void rankedOnlyIncomingDoesNothingForLostConnection() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectRanked(b, a, 1)
					.connectRanked(c, a, 1)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			// only one incoming here
			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipLostConnection(r));

			long targetedIncoming = TraversalUtils.goStream(rf.getTargetedIncoming(startNode)).count();

			// nothing really happens, because only ranked incoming
			assertEquals(0, targetedIncoming);

			tx.failure();
		}
	}

	@Test
	public void manualShouldStayTargetedForLostConnection() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectRanked(b, a, 1)
					.connectManual(c, a, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipLostConnection(r));

			long targetedIncoming = TraversalUtils.goStream(rf.getTargetedIncoming(startNode)).count();

			// incoming manual connections should remain unchanged
			assertEquals(1, targetedIncoming);
			assertTrue(cNode.hasRelationship(rf.getManualType()));
			assertEquals(startNode, cNode.getSingleRelationship(rf.getManualType(), Direction.OUTGOING).getEndNode());

			tx.failure();
		}
	}

	@Test
	public void provisionalsShouldDisappearForLostConnection() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectProvisional(b, a, rf)
					.connectProvisional(c, a, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> adjuster.flipLostConnection(r));

			long targetedIncoming = TraversalUtils.goStream(rf.getTargetedIncoming(startNode)).count();

			// incoming provisional connections should disappear
			assertEquals(0, targetedIncoming);
			assertFalse(bNode.hasRelationship(rf.getProvisionalType()));
			assertFalse(cNode.hasRelationship(rf.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void lostConnectionCreatesCycle() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.connectRanked(a, d, 1)
					.connectRanked(a, b, 2)
					.connectRanked(b, c, 1)
					.connectRanked(c, a, 1)
					.connectProvisional(a, d, rf)
					.connectProvisional(b, c, rf)
					.connectProvisional(c, a, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			// The premise is that d loses connection.
			// Removing the a-->d relationship should create the a-provisional->b
			// relationship, because rank.
			// This creates a cycle c->b->a
			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			rf.getTargetedOutgoing(aNode).ifPresent(r -> adjuster.flipLostConnection(r));

			// the cycle should clear all provisional relationships
			assertFalse(aNode.hasRelationship(rf.getProvisionalType()));
			assertFalse(bNode.hasRelationship(rf.getProvisionalType()));
			assertFalse(cNode.hasRelationship(rf.getProvisionalType()));

			tx.failure();
		}

	}

	@Test
	public void nonCycleShouldNotChange() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectProvisional(a, b, rf)
					.connectProvisional(b, c, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			adjuster.clearIfCycle(startNode);

			// incoming provisional connections should disappear
			assertEquals(bNode, startNode.getSingleRelationship(rf.getProvisionalType(), Direction.OUTGOING).getEndNode());
			assertEquals(cNode, bNode.getSingleRelationship(rf.getProvisionalType(), Direction.OUTGOING).getEndNode());

			tx.failure();
		}

	}

	@Test
	public void cycleShouldClearProvisionals() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectProvisional(a, b, rf)
					.connectProvisional(b, c, rf)
					.connectProvisional(c, a, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			adjuster.clearIfCycle(startNode);

			// incoming provisional connections should disappear
			assertFalse(startNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));
			assertFalse(bNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));
			assertFalse(cNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void cycleShouldClearSlashFlipIncomingNonCyclic() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String nonCyclee = "nonCyclee";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(nonCyclee, 4)
					.connectProvisional(a, b, rf)
					.connectProvisional(b, c, rf)
					.connectProvisional(c, a, rf)
					.connectProvisional(nonCyclee, a, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node nonCycleNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);

			assertTrue(nonCycleNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));

			adjuster.clearIfCycle(startNode);

			// incoming provisional connections should disappear
			assertFalse(startNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));
			assertFalse(bNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));
			assertFalse(cNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));
			assertFalse(nonCycleNode.hasRelationship(Direction.OUTGOING, rf.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void manualCycleShouldNotInfiniteLoop() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectManual(a, b, rf)
					.connectManual(b, c, rf)
					.connectManual(c, a, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			adjuster.clearIfCycle(startNode);

			// incoming provisional connections should disappear
			assertTrue(startNode.hasRelationship(Direction.OUTGOING, rf.getManualType()));
			assertTrue(bNode.hasRelationship(Direction.OUTGOING, rf.getManualType()));
			assertTrue(cNode.hasRelationship(Direction.OUTGOING, rf.getManualType()));

			tx.failure();
		}

	}

	@Test
	public void basicProvisionalTarget() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String connected = "connected";

			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(connected, 4)
					.connectRanked(a, b, 1)
					.connectRanked(a, c, 2)
					.connectProvisional(b, connected, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Optional<Node> provisionalTarget = adjuster.getProvisionalTarget(startNode);

			assertTrue(provisionalTarget.isPresent());
			assertEquals(b, provisionalTarget.get().getProperty("name"));

			tx.failure();
		}
	}

	@Test
	public void provisionalTargetSecondOption() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String connected = "connected";

			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(connected, 4)
					.connectRanked(a, b, 1)
					.connectRanked(a, c, 2)
					.connectProvisional(c, connected, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Optional<Node> provisionalTarget = adjuster.getProvisionalTarget(startNode);

			assertTrue(provisionalTarget.isPresent());
			assertEquals(c, provisionalTarget.get().getProperty("name"));

			tx.failure();
		}
	}

	@Test
	public void provisionalTargetAllRankedUnconnected() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectRanked(a, b, 1)
					.connectRanked(a, c, 2)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Optional<Node> provisionalTarget = adjuster.getProvisionalTarget(startNode);

			assertFalse(provisionalTarget.isPresent());

			tx.failure();
		}
	}


	@Test
	public void testRankedOnlyIsNotConnected() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";

			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.connectRanked(a, b, 1)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = adjuster.isConnected(startNode);

			assertFalse(isConnected);

			tx.failure();
		}
	}

	@Test
	public void testIsConnectedViaAuthored() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String opinion = "opinion";

			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addOpinion(opinion, 2)
					.connectAuthored(a, opinion, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = adjuster.isConnected(startNode);

			assertTrue(isConnected);

			tx.failure();
		}
	}

	@Test
	public void testIsConnectedViaProvisional() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";

			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.connectProvisional(a, b, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = adjuster.isConnected(startNode);

			assertTrue(isConnected);

			tx.failure();
		}
	}

	@Test
	public void testIsConnectedThroughManualConnection() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectManual(a, b, rf)
					.connectProvisional(b, c, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = adjuster.isConnected(startNode);

			assertTrue(isConnected);

			tx.failure();
		}
	}

	@Test
	public void testNoCycleWithProvisionalPlusManual() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectProvisional(a, b, rf)
					.connectManual(b, c, rf)
					.connectRanked(c, a, 1)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = adjuster.getCycle(startNode);

			assertTrue(cycle.isEmpty());

			tx.failure();
		}
	}

	@Test
	public void testNodeIsInCycle() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.connectProvisional(a, b, rf)
					.connectProvisional(b, c, rf)
					.connectProvisional(c, a, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = adjuster.getCycle(startNode);

			assertEquals(3, cycle.size());

			tx.failure();
		}
	}

	@Test
	public void testCycleWithoutNodeOk() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";

			// there's a b->c->d cycle, but a is outside of it
			String acyclicCreate = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.connectProvisional(a, b, rf)
					.connectProvisional(b, c, rf)
					.connectProvisional(c, d, rf)
					.connectProvisional(d, b, rf)
					.build();

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = adjuster.getCycle(startNode);

			assertTrue(cycle.isEmpty());

			tx.failure();
		}
	}
}
