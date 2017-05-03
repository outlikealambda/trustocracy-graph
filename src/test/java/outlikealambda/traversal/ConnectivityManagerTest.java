package outlikealambda.traversal;

import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import outlikealambda.utils.Composables;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class ConnectivityManagerTest {
	@ClassRule
	public static Neo4jRule neo4j = new Neo4jRule();

	private static Function<Integer, Node> getPerson = TestUtils.getPerson(neo4j);

	@Test
	public void testSetRanked() {
		try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
			String a = "a";
			String b = "b";
			String c = "c";
			String d = "d";

			String create = TestUtils.createWalkable(0)
					.addPerson(a, 1)
					.addPerson(b, 2)
					.addPerson(c, 3)
					.addPerson(d, 4)
					.connectRanked(a, b, 1)
					.connectRanked(a, c, 2)
					.connectRanked(a, d, 3)
					.build();

			neo4j.getGraphDatabaseService().execute(create);

			Node aNode = getPerson.apply(1);
			Node bNode = getPerson.apply(2);
			Node cNode = getPerson.apply(3);
			Node dNode = getPerson.apply(4);

			ConnectivityManager.setRanked(aNode, Arrays.asList(
					dNode,
					cNode,
					bNode
			));

			List<Node> rankedOrder = Composables.goStream(aNode.getRelationships(Relationships.Types.ranked()))
					.sorted(Relationships.rankComparator)
					.map(Relationship::getEndNode)
					.collect(toList());

			assertEquals(3, rankedOrder.size());

			// starts -> b, c, d
			// changed to -> d, c, b
			assertEquals(dNode, rankedOrder.get(0));
			assertEquals(cNode, rankedOrder.get(1));
			assertEquals(bNode, rankedOrder.get(2));


			tx.failure();
		}
	}
}
