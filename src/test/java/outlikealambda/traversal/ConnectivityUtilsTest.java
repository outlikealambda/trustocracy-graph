package outlikealambda.traversal;

import org.junit.Rule;
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

	@Rule
	public Neo4jRule neo4j = new Neo4jRule();

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

			ConnectivityUtils.cycleCheck(startNode, topic);

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

			ConnectivityUtils.cycleCheck(startNode, topic);

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

			ConnectivityUtils.cycleCheck(startNode, topic);

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

			ConnectivityUtils.cycleCheck(startNode, topic);

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
			String r = "AUTHORED_1";
			String opinion = "opinion";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					opinion(opinion, 2),
					connect(a, opinion, r))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = ConnectivityUtils.isConnected(startNode, new Relationships.Topic(1));

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

	private static String connectRanked(String a, String b, int rank) {
		return connect(a, b, String.format("RANKED {rank:%d}", rank));
	}
}
