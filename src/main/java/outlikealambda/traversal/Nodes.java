package outlikealambda.traversal;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

public class Nodes {
	public static class Labels {
		public static Label TOPIC = Label.label("Topic");
		public static Label OPINION = Label.label("Opinion");
		public static Label PERSON = Label.label("Person");
	}

	public static class Fields {
		public static String ID = "id";
		public static String DISJOINT = "disjoint";

		public static Long getId(Node node) {
			return (Long) node.getProperty(ID);
		}

		public static boolean isDisjoint(Node node) {
			return node.hasProperty(DISJOINT);
		}

		// todo: is removing faster or is setting to false?
		// how does that affect isDisjoint?
		public static void setDisjoint(Node node, boolean isDisjoint) {
			if (isDisjoint) {
				node.setProperty(DISJOINT, true);
			} else {
				node.removeProperty(DISJOINT);
			}
		}
	}
}
