package outlikealambda.procedure;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FindClosestTraversalTest {
	@Rule
	public Neo4jRule neo4j = new Neo4jRule()
			.withProcedure(FindClosest.class);

	private Driver driver;

	@Before
	public void setup() {
		driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
		try (Session session = driver.session()) {
			session.run(getCreateStatement());
		}
	}

	@Test
	public void basicDistanceTraversal() {
		try (
				Session session = driver.session()
		) {

			StatementResult result = session.run("CALL traverse.distance(0, 0, 4)");

			// expect paths:
			// a -> b -> d
			// a -> b -> e
			// a -> c -> f
			assertEquals(3, countConnected(result));

			result = session.run("CALL traverse.distance(0, 0, 5)");

			// additional path:
			// a -> c -> g
			assertEquals(4, countConnected(result));

			result = session.run("CALL traverse.distance(0, 0, 6)");

			// additional path:
			// a -> c -> g -> h
			assertEquals(5, countConnected(result));
		}
	}

	@Test
	public void multiplePathsAreGrouped() {
		try (
				Session session = driver.session()
		) {

			session.run("MATCH (c:Person{id:2}), (d:Person{id:3}) CREATE (c)-[:TRUSTS_EXPLICITLY]->(d)");

			StatementResult result = session.run("CALL traverse.distance(0, 0, 4)");


			// expect paths:
			// a -> b -> d
			// a -> b -> e
			// a -> c -> f
			// a -> c -> d
			List<Record> records = result.list();
			assertEquals(4, countTotalPaths(records));

		}
	}

	@Test
	public void withUnconnectedReturnsAllOpinions() {
		try (
				Session session = driver.session()
		) {

			session.run("MATCH (c:Person{id:2}), (d:Person{id:3}) CREATE (c)-[:TRUSTS_EXPLICITLY]->(d)");

			// distance of 1 still returns all opinions
			StatementResult result = session.run("CALL traverse.distance.withUnconnected(0, 0, 1)");


			List<Record> records = result.list();
			assertEquals(5, records.size());

		}
	}

	@Test
	public void delegatesRelationshipWithWrongTopicIdIsNotFollowed() {
		try (
				Session session = driver.session()
		) {

			session.run("MATCH (a:Person{id:0}), (g:Person{id:6}) CREATE (a)-[:DELEGATES{topicId:1}]->(g)");

			StatementResult result = session.run("CALL traverse.distance(0, 0, 4)");

			// expect paths:
			// a -> b -> d
			// a -> b -> e
			// a -> c -> f
			assertEquals(3, countConnected(result));
		}
	}

	@Test
	public void delegatesRelationshipWithCorrectTopicIdIsFollowed() {
		try (
				Session session = driver.session()
		) {

			session.run("MATCH (a:Person{id:0}), (g:Person{id:6}) CREATE (a)-[:DELEGATES{topicId:0}]->(g)");

			StatementResult result = session.run("CALL traverse.distance(0, 0, 4)");

			// expect paths:
			// a -> b -> d
			// a -> b -> e
			// a -> c -> f
			// a -> g
			// a -> g -> h
			assertEquals(5, countConnected(result));
		}
	}

	@Test
	public void testInfluence() {
		try (
				Session session = driver.session()
		) {

			StatementResult result;

			result = session.run("CALL influence.person(7, 0, 1)");
			assertEquals(1, result.single().get("influence").asInt());

			result = session.run("CALL influence.person(7, 0, 2)");
			assertEquals(2, result.single().get("influence").asInt());

			result = session.run("CALL influence.person(7, 0, 4)");
			assertEquals(3, result.single().get("influence").asInt());

			result = session.run("CALL influence.person(7, 0, 6)");
			assertEquals(4, result.single().get("influence").asInt());
		}
	}

	@Test
	public void testInfluenceCountsDelegateRelationshipsAsZero() {
		try (
				Session session = driver.session()
		) {

			session.run("MATCH (a:Person{id:0}), (g:Person{id:6}) CREATE (a)-[:DELEGATES{topicId:0}]->(g)");

			// should now have two nodes (reflexive + delegate) less than one from the user
			StatementResult result = session.run("CALL influence.person(6, 0, 1)");
			assertEquals(2, result.single().get("influence").asInt());
		}
	}

	@Test
	public void testInfluenceFromOpinionId() {
		try (
				Session session = driver.session()
		) {

			session.run("MATCH (b:Person{id:0}), (g:Person{id:6}) CREATE (a)-[:DELEGATES{topicId:0}]->(g)");

			/**
			 * both h:Person {id:7} and oh:Opinion {id:7} should have 5 nodes within a
			 * distance of 6: [h, g, c, a, b] (b is directly delegated above)
			 */
			StatementResult personResult = session.run("CALL influence.person(7, 0, 6)");
			assertEquals(5, personResult.single().get("influence").asInt());

			StatementResult opinionResult = session.run("CALL influence.opinion(7, 6)");
			assertEquals(5, opinionResult.single().get("influence").asInt());
		}
	}

	/**
	 *     d --- od
	 *    /        \
	 *   b - e - oe |
	 *  /          \|
	 * a            T
	 *  \          /| \
	 *   c - f - of |  |
	 *    \         /  |
	 *     g - h - oh /
	 *      \        /
	 *       og ----
	 *
	 */
	private static String getCreateStatement() {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE (a:Person {name:'A', id:0})")
				.append(", (b:Person {name:'B', id:1})")
				.append(", (c:Person {name:'C', id:2})")
				.append(", (d:Person {name:'D', id:3})")
				.append(", (e:Person {name:'E', id:4})")
				.append(", (f:Person {name:'F', id:5})")
				.append(", (g:Person {name:'G', id:6})")
				.append(", (h:Person {name:'H', id:7})")
				.append(", (od:Opinion {id:3, text: '3', influence: 33})")
				.append(", (oe:Opinion {id:4, text: '4', influence: 44})")
				.append(", (of:Opinion {id:5, text: '5', influence: 55})")
				.append(", (og:Opinion {id:6, text: '6', influence: 66})")
				.append(", (oh:Opinion {id:7, text: '7', influence: 77})")
				.append(", (t:Topic {id:0})")

				// link people to opinions
				.append(", (d)-[:OPINES]->(od)")
				.append(", (e)-[:OPINES]->(oe)")
				.append(", (f)-[:OPINES]->(of)")
				.append(", (g)-[:OPINES]->(og)")
				.append(", (h)-[:OPINES]->(oh)")

				// link opinions to topic
				.append(", (od)-[:ADDRESSES]->(t)")
				.append(", (oe)-[:ADDRESSES]->(t)")
				.append(", (of)-[:ADDRESSES]->(t)")
				.append(", (og)-[:ADDRESSES]->(t)")
				.append(", (oh)-[:ADDRESSES]->(t)")

				// link people
				.append(", (a)-[:TRUSTS_EXPLICITLY]->(b)")
				.append(", (a)-[:TRUSTS]->(c)")
				.append(", (b)-[:TRUSTS_EXPLICITLY]->(d)")
				.append(", (b)-[:TRUSTS]->(e)")
				.append(", (c)-[:TRUSTS_EXPLICITLY]->(f)")
				.append(", (c)-[:TRUSTS]->(g)")
				.append(", (g)-[:TRUSTS_EXPLICITLY]->(h)")

				.append("");


		return sb.toString();
	}

	private int countTotalPaths(List<Record> records) {
		return records.stream()
//				.map(record -> {
//					System.out.println(record.get("author"));
//					System.out.println(record);
//					return record;
//				})
				.map(record -> record.get("paths"))
				.filter(v -> !v.isNull())
//				.map(journeys -> {
//					System.out.println("hi");
//					System.out.println(journeys.size());
//					journeys.asList(j -> j.asMap()).forEach(j -> System.out.println(j.get("trustee")));
//					return journeys;
//				})
				.map(Value::asList)
				.mapToInt(List::size)
				.sum();
	}

	private static long countConnected(StatementResult result) {
		return result.list().stream()
				.map(record -> record.get("paths"))
				.filter(v -> !v.isNull())
				.count();
	}
}
