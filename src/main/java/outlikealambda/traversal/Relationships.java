package outlikealambda.traversal;

import org.neo4j.graphdb.RelationshipType;

public final class Relationships {
	private static final String MANUAL = "MANUAL";
	private static final String PROVISIONAL = "PROVISIONAL";

	public static class Types {
		public static RelationshipType manual(int topic) {
			return combineTypeAndId(MANUAL, topic);
		}

		public static RelationshipType provisional(int topic) {
			return combineTypeAndId(PROVISIONAL, topic);
		}

		private static RelationshipType combineTypeAndId(String s, int id) {
			return RelationshipType.withName(s + "_" + id);
		}
	}

	private Relationships() {}
}
