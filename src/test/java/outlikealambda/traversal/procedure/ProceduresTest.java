package outlikealambda.traversal.procedure;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.procedure.DirtyConnectivity;
import outlikealambda.procedure.Traverse;
import outlikealambda.traversal.TestUtils;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static outlikealambda.traversal.TestUtils.containsFriendAuthorNameCombo;
import static outlikealambda.traversal.TestUtils.friendIsInfluencer;

public class ProceduresTest {
	@Rule
	public Neo4jRule neo4j = new Neo4jRule()
			.withProcedure(DirtyConnectivity.class)
			.withProcedure(Traverse.class);

	@Test
	public void testDefaultTrustPath() {
		try (
				Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()
		) {
			String klb = "klb";
			String mb = "mb";
			String ng = "ng";
			String sr = "sr";
			String ll = "ll";
			String o = "opinion";

			String create = TestUtils.createWalkable(0)
					.addPerson(klb, 1)
					.addPerson(mb, 2)
					.addPerson(ng, 3)
					.addPerson(sr, 4)
					.addPerson(ll, 5)
					.addOpinion(o, 0)
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

			session.run(create);

			// void
			session.run("CALL dirty.opinion.set(1, 0, 0)");
			StatementResult result = session.run("CALL friend.author.opinion(2, 0)");

			List<Record> results = result.list();

			assertFalse(results.isEmpty());
			assertEquals(4, results.size());

			// sr -> klb
			assertTrue(containsFriendAuthorNameCombo("sr", "klb", results));
			assertTrue(friendIsInfluencer("sr", results));
		}
	}

	@Test
	public void testManualTrustPathReturnsRankedIfManualAndRanked() {
		try (
				Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()
		) {
			String klb = "klb";
			String mb = "mb";
			String ng = "ng";
			String sr = "sr";
			String ll = "ll";
			String o1 = "opinion1";
			String o2 = "opinion2";

			String create = TestUtils.createWalkable(0)
					.addPerson(klb, 1)
					.addPerson(mb, 2)
					.addPerson(ng, 3)
					.addPerson(sr, 4)
					.addPerson(ll, 5)
					.addOpinion(o1, 1)
					.addOpinion(o2, 2)
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

			session.run(create);

			// void
			session.run("CALL dirty.opinion.set(1, 1, 0)");
			session.run("CALL dirty.opinion.set(5, 2, 0)");

			session.run("CALL dirty.target.set(2, 5, 0)");

			StatementResult result = session.run("CALL friend.author.opinion(2, 0)");

			List<Record> results = result.list();

			assertFalse(results.isEmpty());
			assertEquals(4, results.size());

			// sr -> klb
			assertTrue(containsFriendAuthorNameCombo("ll", "ll", results));
			assertTrue(friendIsInfluencer("ll", results));
		}
	}
}


