package outlikealambda.procedure;

import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.traversal.RelationshipFilter;
import outlikealambda.traversal.TestUtils;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static outlikealambda.traversal.TestUtils.containsFriendAuthorNameCombo;
import static outlikealambda.traversal.TestUtils.containsFriendWithoutAuthor;
import static outlikealambda.traversal.TestUtils.friendIsInfluencer;

public class SelectiveConnectivityTest {
	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule()
			.withProcedure(SelectiveConnectivity.class);

	@Test
	public void friendAuthorOpinion() {
		try (
				Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()
		) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";
			String z = "z";
			String opinion = "opinion";

			RelationshipFilter rf = new RelationshipFilter(1);

			String create = new TestUtils.Builder()
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.addPerson(z, 5)
					.addOpinion(opinion, 1)
					.connectRanked(a, z, 1)
					.connectRanked(a, c, 2)
					.connectRanked(a, b, 3)
					.connectManual(a, b, rf)
					.connectProvisional(b, d, rf)
					.connectProvisional(c, d, rf)
					.connectAuthored(d, opinion, rf)
					.build();

			session.run(create);

			List<Record> results = session.run("CALL selective.friend.author.opinion(1, 1)").list();

			assertTrue(containsFriendAuthorNameCombo(b, d, results));
			assertTrue(containsFriendAuthorNameCombo(c, d, results));
			assertTrue(containsFriendWithoutAuthor(z, results));
			assertTrue(friendIsInfluencer(b, results));
			assertFalse(friendIsInfluencer(c, results));
			assertFalse(friendIsInfluencer(z, results));
		}
	}
}
