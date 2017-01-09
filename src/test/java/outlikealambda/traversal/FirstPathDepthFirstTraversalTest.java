package outlikealambda.traversal;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.List;

public class FirstPathDepthFirstTraversalTest {
	@Rule
	public Neo4jRule neo4j = new Neo4jRule()
			.withProcedure(FirstPathDepthFirstTraversal.class);


	@Test
	public void testDefaultTrustPath() {
		try (
				Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()
		) {

			session.run(getCreateStatement());
			StatementResult result = session.run("CALL traverse.first(0, 0)");

			List<Record> results = result.list();

			Assert.assertFalse(results.isEmpty());
			Assert.assertEquals(2, results.size());

			System.out.println(results.get(0).toString());
			System.out.println(results.get(1).toString());

			// a -> b -> d
			Assert.assertTrue(containsFriendAuthorNameCombo("B", "D", results));

			// a -> c -> g -> h is the correct path (because of rank)
			// even though a -> c -> f is shorter
			Assert.assertTrue(containsFriendAuthorNameCombo("C", "H", results));
		}
	}

	@Test
	public void testDelegationChangesAuthor() {
		try (
				Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()
		) {

			session.run(getCreateStatement());

			// this checks topic 1
			// C delegates to F with relationship TOPIC_1
			List<Record> results = session.run("CALL traverse.first(0, 1)").list();

			Assert.assertFalse(results.isEmpty());
			Assert.assertEquals(2, results.size());

			System.out.println(results.get(0).toString());
			System.out.println(results.get(1).toString());

			// a -> c -> f is the correct path (because of delegate via TOPIC_1)
			Assert.assertTrue(containsFriendAuthorNameCombo("C", "F", results));
		}
	}

	@Test
	public void gHasNoAuthorThroughI() {
		try (
				Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()
		) {

			session.run(getCreateStatement());

			// Traversal starts with G
			List<Record> results = session.run("CALL traverse.first(6, 0)").list();

			Assert.assertFalse(results.isEmpty());
			Assert.assertEquals(2, results.size());

			System.out.println(results.get(0).toString());
			System.out.println(results.get(1).toString());

			// G has a path through H (H is author)
			Assert.assertTrue(containsFriendAuthorNameCombo("H", "H", results));

			// G has no path through I
			Assert.assertTrue(containsFriendWithoutAuthor("I", results));
		}

	}

	@Test
	public void circularPaths() {
		try (
				Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()
		) {
			session.run(getCreateStatement());

			// Traversal starts with G
			List<Record> results = session.run("CALL traverse.first(7, 0)").list();

			Assert.assertFalse(results.isEmpty());
			Assert.assertEquals(1, results.size());

			System.out.println(results.get(0).toString());

			// J circularly links to G, and so shouldn't point back to H's own opinion
			Assert.assertTrue(containsFriendWithoutAuthor("J", results));
		}

	}

	/**
	 *     d --- od
	 *    /        \
	 *   b - e - oe |
	 *  /          \|
	 * a            T
	 *  \          /|
	 *   c - f - of |
	 *    \         |
	 *     g - h ----
	 *      \   \
	 *        i  j ---- (back to g...)
	 *
	 *
	 */
	private String getCreateStatement() {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE (a:Person {name:'A', id:0})")
				.append(", (b:Person {name:'B', id:1})")
				.append(", (c:Person {name:'C', id:2})")
				.append(", (d:Person {name:'D', id:3})")
				.append(", (e:Person {name:'E', id:4})")
				.append(", (f:Person {name:'F', id:5})")
				.append(", (g:Person {name:'G', id:6})")
				.append(", (h:Person {name:'H', id:7})")
				.append(", (i:Person {name:'I', id:8})")
				.append(", (j:Person {name:'J', id:9})")
				.append(", (od:Opinion {id:3, text: '3', influence: 33})")
				.append(", (oe:Opinion {id:4, text: '4', influence: 44})")
				.append(", (of:Opinion {id:5, text: '5', influence: 55})")
				.append(", (oh:Opinion {id:6, text: '6', influence: 66})")
				.append(", (t0:Topic {id:0})")
				.append(", (t1:Topic {id:1})")

				// link people to opinions
				.append(", (d)-[:OPINES]->(od)")
				.append(", (e)-[:OPINES]->(oe)")
				.append(", (f)-[:OPINES]->(of)")
				.append(", (h)-[:OPINES]->(oh)")

				// link opinions to topic
				.append(", (od)-[:ADDRESSES]->(t0)")
				.append(", (oe)-[:ADDRESSES]->(t0)")
				.append(", (of)-[:ADDRESSES]->(t0)")
				.append(", (oh)-[:ADDRESSES]->(t0)")

				// link opinions to topic
				.append(", (od)-[:ADDRESSES]->(t1)")
				.append(", (oe)-[:ADDRESSES]->(t1)")
				.append(", (of)-[:ADDRESSES]->(t1)")
				.append(", (oh)-[:ADDRESSES]->(t1)")

				// link people
				.append(", (a)-[:TRUSTS {rank:1}]->(b)")
				.append(", (a)-[:TRUSTS {rank:2}]->(c)")
				.append(", (b)-[:TRUSTS {rank:1}]->(d)")
				.append(", (b)-[:TRUSTS {rank:2}]->(e)")
				.append(", (c)-[:TRUSTS {rank:1}]->(g)")
				.append(", (c)-[:TRUSTS {rank:2}]->(f)")
				.append(", (g)-[:TRUSTS {rank:1}]->(h)")
				.append(", (g)-[:TRUSTS {rank:2}]->(i)")
				.append(", (h)-[:TRUSTS {rank:1}]->(j)")
				.append(", (j)-[:TRUSTS {rank:1}]->(g)")
				.append(", (c)-[:TOPIC_1]->(f)")

				.append("");


		return sb.toString();
	}

	private boolean containsFriendAuthorNameCombo(String friendName, String authorName, List<Record> records) {
		return records.stream()
				.filter(r -> r.get("friend").asMap().get("name").equals(friendName))
				.anyMatch(r -> r.get("author").asMap().get("name").equals(authorName));
	}

	private boolean containsFriendWithoutAuthor(String friendName, List<Record> records) {
		return records.stream()
				.filter(r -> r.get("friend").asMap().get("name").equals(friendName))
				.anyMatch(r -> r.get("author").asMap().get("name") == null);
	}

}
