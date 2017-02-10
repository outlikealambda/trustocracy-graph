package outlikealambda.traversal;

import org.neo4j.graphdb.Label;

public class Nodes {
	public static class Labels {
		public static Label OPINION = Label.label("Opinion");
		public static Label PERSON = Label.label("Person");
	}

	public static class Fields {
		public static String ID = "id";
		public static String DISJOINT = "disjoint";
	}
}
