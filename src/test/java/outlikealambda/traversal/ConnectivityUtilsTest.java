package outlikealambda.traversal;

import org.junit.Rule;
import org.junit.Test;
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
			String r = "RANKED";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					connect(a, b, r))
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
			String r = "PROVISIONAL_1";
			String b = "b";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					connect(a, b, r))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = ConnectivityUtils.isConnected(startNode, new Relationships.Topic(1));

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
			String r1= "MANUAL_1";
			String r2= "PROVISIONAL_1";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 2),
					connect(a, b, r1),
					connect(b, c, r2))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			boolean isConnected = ConnectivityUtils.isConnected(startNode, new Relationships.Topic(1));

			assertTrue(isConnected);

			tx.failure();
		}
	}

	@Test
	public void testNoCycle() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String r = "PROVISIONAL_1";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connect(a, b, r),
					connect(b, c, r))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, new Relationships.Topic(1));

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
			String r = "PROVISIONAL_1";

			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					connect(a, b, r),
					connect(b, c, r),
					connect(c, a, r))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, new Relationships.Topic(1));

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
			String r = "PROVISIONAL_1";

			// there's a b->c->d cycle, but a is outside of it
			String acyclicCreate = Stream.of(
					"CREATE" + person(a, 1),
					person(b, 2),
					person(c, 3),
					person(d, 3),
					connect(a, b, r),
					connect(b, c, r),
					connect(c, d, r),
					connect(d, b, r))
					.collect(Collectors.joining(", "));

			neo4j.getGraphDatabaseService().execute(acyclicCreate);

			Node startNode = neo4j.getGraphDatabaseService().findNode(Label.label("Person"), "id", 1);

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, new Relationships.Topic(1));

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

	private static String connectRanked(String a, String b, int rank) {
		return connect(a, b, String.format("RANKED {rank:%d}", rank));
	}
}
