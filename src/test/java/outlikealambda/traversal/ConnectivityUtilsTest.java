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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectivityUtilsTest {

	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	@Test
	public void getDesignatedAuthor() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String z = "z";
			String opinion = "opinion";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 4),
					person(z, 5),
					opinion(opinion, 1),
					connectRanked(a, b, 1),
					connectRanked(b, d, 1),
					connectRanked(z, a, 1),
					connectProvisional(a, c, topic),
					connectProvisional(b, c, topic),
					connectProvisional(c, d, topic),
					connectAuthored(d, opinion, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node dNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);
			Node zNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 5);

			// should not count z-RANKED->a as influence
			assertEquals(dNode, ConnectivityUtils.getDesignatedAuthor(aNode, topic).get());
			assertEquals(dNode, ConnectivityUtils.getDesignatedAuthor(bNode, topic).get());
			assertEquals(dNode, ConnectivityUtils.getDesignatedAuthor(cNode, topic).get());
			assertEquals(dNode, ConnectivityUtils.getDesignatedAuthor(dNode, topic).get());
			assertFalse(ConnectivityUtils.getDesignatedAuthor(zNode, topic).isPresent());

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 4),
					person(z, 5),
					connectRanked(a, b, 1),
					connectRanked(b, d, 1),
					connectRanked(z, a, 1),
					connectProvisional(a, c, topic),
					connectProvisional(b, c, topic),
					connectProvisional(c, d, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node dNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);
			Node zNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 5);

			// should not count z-RANKED->a as influence
			assertEquals(1, ConnectivityUtils.calculateInfluence(aNode, topic));
			assertEquals(1, ConnectivityUtils.calculateInfluence(bNode, topic));
			assertEquals(3, ConnectivityUtils.calculateInfluence(cNode, topic));
			assertEquals(4, ConnectivityUtils.calculateInfluence(dNode, topic));
			assertEquals(1, ConnectivityUtils.calculateInfluence(zNode, topic));

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 4),
					opinion(o, 1),
					connectRanked(a, b, 1),
					connectRanked(b, d, 1),
					connectAuthored(d, o, topic),
					connectProvisional(a, b, topic),
					connectProvisional(b, d, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			// creating b-MANUAL->c should delete b-PROVISIONAL->c and a-PROVISIONAL->b
			ConnectivityUtils.setTarget(bNode, cNode, topic);

			// the cycle should cause all provisional relationships to clear
			assertFalse(topic.getTargetedOutgoing(aNode).isPresent());
			assertFalse(bNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));
			assertTrue(bNode.hasRelationship(Direction.OUTGOING, topic.getManualType()));
			assertTrue(cNode.hasRelationship(Direction.INCOMING, topic.getManualType()));

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 4),
					opinion(o, 1),
					connectRanked(a, b, 1),
					connectRanked(b, c, 1),
					connectRanked(c, a, 1),
					connectManual(a, c, topic),
					connectAuthored(d, o, topic),
					connectProvisional(b, c, topic),
					connectProvisional(c, a, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node dNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);

			// deleting a-MANUAL->d should connect a-PROVISIONAL->b and create a cycle
			ConnectivityUtils.clearTarget(aNode, topic);

			// the cycle should cause all provisional relationships to clear
			assertFalse(topic.getTargetedOutgoing(aNode).isPresent());
			assertFalse(topic.getTargetedOutgoing(bNode).isPresent());
			assertFalse(topic.getTargetedOutgoing(cNode).isPresent());

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 4),
					opinion(o, 1),
					connectRanked(a, c, 1),
					connectRanked(d, a, 1),
					connectManual(a, b, topic),
					connectAuthored(c, o, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node dNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);

			// deleting a-MANUAL->b should connect a-PROVISIONAL->c and d-PROVISIONAL->a
			ConnectivityUtils.clearTarget(aNode, topic);

			// b should still manually point to a
			assertEquals(cNode, topic.getTargetedOutgoing(aNode).get().getEndNode());
			assertEquals(aNode, topic.getTargetedOutgoing(dNode).get().getEndNode());
			assertTrue(aNode.hasRelationship(topic.getProvisionalType(), Direction.OUTGOING));
			assertTrue(cNode.hasRelationship(topic.getProvisionalType(), Direction.INCOMING));
			assertTrue(dNode.hasRelationship(topic.getProvisionalType(), Direction.OUTGOING));
			assertTrue(aNode.hasRelationship(topic.getProvisionalType(), Direction.INCOMING));

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					opinion(o, 1),
					connectRanked(a, b, 1),
					connectAuthored(c, o, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			// connecting b-MANUAL->c should connect a-PROVISIONAL->b
			ConnectivityUtils.setTarget(bNode, cNode, topic);

			// b should still manually point to a
			assertEquals(cNode, topic.getTargetedOutgoing(bNode).get().getEndNode());
			assertTrue(aNode.hasRelationship(topic.getProvisionalType(), Direction.OUTGOING));
			assertTrue(bNode.hasRelationship(topic.getProvisionalType(), Direction.INCOMING));

			tx.failure();
		}

	}

	@Test
	public void switchManualTargets() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectManual(a, b, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			ConnectivityUtils.setTarget(aNode, cNode, topic);

			// b should still manually point to a
			assertEquals(cNode, topic.getTargetedOutgoing(aNode).get().getEndNode());

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionShouldFlipChildrenOfManual() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectManual(b, a, topic),
					connectRanked(c, b, 1))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipGainedConnection(r, topic));

			// b should still manually point to a
			assertEquals(startNode, bNode.getSingleRelationship(topic.getManualType(), Direction.OUTGOING).getEndNode());

			// c should now provisionally point to b
			assertEquals(bNode, cNode.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING).getEndNode());

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionShouldNotProvisionRankedIncomingWithOtherManual() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectRanked(b, a, 1),
					connectManual(b, c, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipGainedConnection(r, topic));

			// b should still manually point to c
			assertEquals(cNode, bNode.getSingleRelationship(topic.getManualType(), Direction.OUTGOING).getEndNode());

			// b should still have a ranked connection to a
			assertEquals(startNode, bNode.getSingleRelationship(topic.getRankedType(), Direction.OUTGOING).getEndNode());

			tx.failure();
		}
	}

	@Test
	public void gainedConnectionShouldNotProvisionRankedIncomingWithHigherProvisional() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectRanked(b, a, 2),
					connectRanked(b, c, 1),
					connectProvisional(b, c, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipGainedConnection(r, topic));

			// b should still provisionally point to c
			assertEquals(cNode, bNode.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING).getEndNode());

			// incoming to startNode should still be ranked from b
			assertEquals(bNode, startNode.getSingleRelationship(topic.getRankedType(), Direction.INCOMING).getStartNode());

			assertFalse(startNode.hasRelationship(topic.getProvisionalType()));

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectRanked(b, a, 1),
					connectRanked(b, c, 2),
					connectProvisional(b, c, topic),
					connectAuthored(a, opinion, topic),
					connectAuthored(c, opinion, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipGainedConnection(r, topic));

			// b should now provisionally point to a
			assertEquals(startNode, bNode.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING).getEndNode());

			// incoming to cNode should be ranked from b
			assertEquals(bNode, cNode.getSingleRelationship(topic.getRankedType(), Direction.INCOMING).getStartNode());

			assertFalse(cNode.hasRelationship(topic.getProvisionalType()));

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

			Relationships.Topic topic = new Relationships.Topic(1);

			/*
			 * This is the starting state of the graph.  What kicks it off is
			 * b switching from manual->e to manual->c.  This causes a-provisional->b,
			 * which causes c-provisional->a.
			 *
			 * This should trigger a cycle and the deletion of all provisional relationships
			 *
			 */
			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 4),
					person(e, 5),
					connectRanked(a, b, 1),
					connectRanked(b, c, 1),
					connectRanked(c, a, 1),
					connectRanked(c, d, 2),
					connectManual(b, e, topic),
					connectProvisional(c, d, topic),
					connectAuthored(d, opinion, topic))
					.collect(Collectors.joining(", "));

			// starting state
			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			// delete starting b-manual->e connection
			bNode.getSingleRelationship(topic.getManualType(), Direction.OUTGOING).delete();

			// create b-manual->c connection
			bNode.createRelationshipTo(cNode, topic.getManualType());

			// this should trigger the a-flip, then the c-flip, and then... cycle!
			bNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipGainedConnection(r, topic));

			// b should manually point to c
			assertEquals(cNode, bNode.getSingleRelationship(topic.getManualType(), Direction.OUTGOING).getEndNode());

			// provisionals should be gone from c->a, a->b
			assertFalse(cNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));
			assertFalse(aNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectRanked(b, a, 2),
					connectRanked(b, c, 1),
					connectAuthored(a, opinion, topic),
					connectAuthored(c, opinion, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipGainedConnection(r, topic));

			// b should now provisionally point to a
			assertEquals(startNode, bNode.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING).getEndNode());

			// incoming to cNode should be ranked from b
			assertEquals(bNode, cNode.getSingleRelationship(topic.getRankedType(), Direction.INCOMING).getStartNode());

			assertFalse(cNode.hasRelationship(topic.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void rankedOnlyIncomingDoesNothingForLostConnection() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectRanked(b, a, 1),
					connectRanked(c, a, 1))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Relationships.Topic topic = new Relationships.Topic(1);

			// only one incoming here
			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipLostConnection(r, topic));

			long targetedIncoming = TraversalUtils.goStream(topic.getTargetedIncoming(startNode)).count();

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectRanked(b, a, 1),
					connectManual(c, a, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipLostConnection(r, topic));

			long targetedIncoming = TraversalUtils.goStream(topic.getTargetedIncoming(startNode)).count();

			// incoming manual connections should remain unchanged
			assertEquals(1, targetedIncoming);
			assertTrue(cNode.hasRelationship(topic.getManualType()));
			assertEquals(startNode, cNode.getSingleRelationship(topic.getManualType(), Direction.OUTGOING).getEndNode());

			tx.failure();
		}
	}

	@Test
	public void provisionalsShouldDisappearForLostConnection() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectProvisional(b, a, topic),
					connectProvisional(c, a, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			startNode.getRelationships(Direction.INCOMING)
					.forEach(r -> ConnectivityUtils.flipLostConnection(r, topic));

			long targetedIncoming = TraversalUtils.goStream(topic.getTargetedIncoming(startNode)).count();

			// incoming provisional connections should disappear
			assertEquals(0, targetedIncoming);
			assertFalse(bNode.hasRelationship(topic.getProvisionalType()));
			assertFalse(cNode.hasRelationship(topic.getProvisionalType()));

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 4),
					connectRanked(a, d, 1),
					connectRanked(a, b, 2),
					connectRanked(b, c, 1),
					connectRanked(c, a, 1),
					connectProvisional(a, d, topic),
					connectProvisional(b, c, topic),
					connectProvisional(c, a, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			// The premise is that d loses connection.
			// Removing the a-->d relationship should create the a-provisional->b
			// relationship, because rank.
			// This creates a cycle c->b->a
			Node aNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			topic.getTargetedOutgoing(aNode).ifPresent(r -> ConnectivityUtils.flipLostConnection(r, topic));

			// the cycle should clear all provisional relationships
			assertFalse(aNode.hasRelationship(topic.getProvisionalType()));
			assertFalse(bNode.hasRelationship(topic.getProvisionalType()));
			assertFalse(cNode.hasRelationship(topic.getProvisionalType()));

			tx.failure();
		}

	}

	@Test
	public void nonCycleShouldNotChange() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectProvisional(a, b, topic),
					connectProvisional(b, c, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			ConnectivityUtils.clearIfCycle(startNode, topic);

			// incoming provisional connections should disappear
			assertEquals(bNode, startNode.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING).getEndNode());
			assertEquals(cNode, bNode.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING).getEndNode());

			tx.failure();
		}

	}

	@Test
	public void cycleShouldClearProvisionals() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectProvisional(a, b, topic),
					connectProvisional(b, c, topic),
					connectProvisional(c, a, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			ConnectivityUtils.clearIfCycle(startNode, topic);

			// incoming provisional connections should disappear
			assertFalse(startNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));
			assertFalse(bNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));
			assertFalse(cNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(nonCyclee, 4),
					connectProvisional(a, b, topic),
					connectProvisional(b, c, topic),
					connectProvisional(c, a, topic),
					connectProvisional(nonCyclee, a, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);
			Node nonCycleNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 4);

			assertTrue(nonCycleNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));

			ConnectivityUtils.clearIfCycle(startNode, topic);

			// incoming provisional connections should disappear
			assertFalse(startNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));
			assertFalse(bNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));
			assertFalse(cNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));
			assertFalse(nonCycleNode.hasRelationship(Direction.OUTGOING, topic.getProvisionalType()));

			tx.failure();
		}
	}

	@Test
	public void manualCycleShouldNotInfiniteLoop() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectManual(a, b, topic),
					connectManual(b, c, topic),
					connectManual(c, a, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);
			Node bNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 2);
			Node cNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 3);

			ConnectivityUtils.clearIfCycle(startNode, topic);

			// incoming provisional connections should disappear
			assertTrue(startNode.hasRelationship(Direction.OUTGOING, topic.getManualType()));
			assertTrue(bNode.hasRelationship(Direction.OUTGOING, topic.getManualType()));
			assertTrue(cNode.hasRelationship(Direction.OUTGOING, topic.getManualType()));

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

			String provisional = "PROVISIONAL_1";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(connected, 4),
					connectRanked(a, b, 1),
					connectRanked(a, c, 2),
					connect(b, connected, provisional))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Optional<Node> provisionalTarget = ConnectivityUtils.getProvisionalTarget(startNode, new Relationships.Topic(1));

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

			String provisional = "PROVISIONAL_1";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(connected, 4),
					connectRanked(a, b, 1),
					connectRanked(a, c, 2),
					connect(c, connected, provisional))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Optional<Node> provisionalTarget = ConnectivityUtils.getProvisionalTarget(startNode, new Relationships.Topic(1));

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

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectRanked(a, b, 1),
					connectRanked(a, c, 2))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Optional<Node> provisionalTarget = ConnectivityUtils.getProvisionalTarget(startNode, new Relationships.Topic(1));

			assertFalse(provisionalTarget.isPresent());

			tx.failure();
		}
	}


	@Test
	public void testRankedOnlyIsNotConnected() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					connectRanked(a, b, 1))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = ConnectivityUtils.isConnected(startNode, new Relationships.Topic(1));

			assertFalse(isConnected);

			tx.failure();
		}
	}

	@Test
	public void testIsConnectedViaAuthored() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String opinion = "opinion";
			Relationships.Topic topic = new Relationships.Topic(1);

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					opinion(opinion, 2),
					connectAuthored(a, opinion, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = ConnectivityUtils.isConnected(startNode, topic);

			assertTrue(isConnected);

			tx.failure();
		}
	}

	@Test
	public void testIsConnectedViaProvisional() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";

			Relationships.Topic topic = new Relationships.Topic(1);

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					connectProvisional(a, b, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = ConnectivityUtils.isConnected(startNode, topic);

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
			Relationships.Topic topic = new Relationships.Topic(1);

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 2),
					connectManual(a, b, topic),
					connectProvisional(b, c, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = ConnectivityUtils.isConnected(startNode, topic);

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectProvisional(a, b, topic),
					connectManual(b, c, topic),
					connectRanked(c, a, 1))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, topic);

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

			Relationships.Topic topic = new Relationships.Topic(1);

			String create = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connectProvisional(a, b, topic),
					connectProvisional(b, c, topic),
					connectProvisional(c, a, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(create);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, topic);

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

			Relationships.Topic topic = new Relationships.Topic(1);

			// there's a b->c->d cycle, but a is outside of it
			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 3),
					connectProvisional(a, b, topic),
					connectProvisional(b, c, topic),
					connectProvisional(c, d, topic),
					connectProvisional(d, b, topic))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, topic);

			assertTrue(cycle.isEmpty());

			tx.failure();
		}
	}


	private static String person(String a, int id) {
		return String.format("(%s:Person {name:'%s', id:%d})", a, a, id);
	}

	private static String opinion(String a, int id) {
		return String.format("(%s:Opinion {id:%d})", a, id);
	}

	private static String connect(String a, String b, String r) {
		return String.format("(%s)-[:%s]->(%s)", a, r, b);
	}

	private static String connectManual(String a, String b, Relationships.Topic topic) {
		return connect(a, b, topic.getManualType().name());
	}

	private static String connectProvisional(String a, String b, Relationships.Topic topic) {
		return connect(a, b, topic.getProvisionalType().name());
	}

	private static String connectAuthored(String a, String b, Relationships.Topic topic) {
		return connect(a, b, topic.getAuthoredType().name());
	}

	private static String connectRanked(String a, String b, int rank) {
		return connect(a, b, String.format("RANKED {rank:%d}", rank));
	}
}
