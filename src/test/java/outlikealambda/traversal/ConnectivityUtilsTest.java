package outlikealambda.traversal;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.Relationships.Topic;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConnectivityUtilsTest {

	@Rule
	public Neo4jRule neo4j = new Neo4jRule();

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

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, new Topic(1));

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

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, 1);

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

			Collection<Node> cycle = ConnectivityUtils.getCycle(startNode, 1);

			assertTrue(cycle.isEmpty());

			tx.failure();
		}

	}


	private static String person(String a, int id) {
		return String.format("(%s:Person {name:'%s', id:%d})", a, a, id);
	}

	private static String connect(String a, String b, String r) {
		return String.format("(%s)-[:%s]->(%s)", a, r, b);
	}
}
