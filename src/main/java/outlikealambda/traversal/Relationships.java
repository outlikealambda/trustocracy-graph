package outlikealambda.traversal;

import org.neo4j.graphdb.RelationshipType;

public final class Relationships {
	public final static class Types {
		private static final String MANUAL = "MANUAL";
		private static final String AUTHORED = "AUTHORED";
		private static final String RANKED = "RANKED";
		private static final String CONNECTED = "CONNECTED";

		private static final RelationshipType RANKED_TYPE = RelationshipType.withName(RANKED);

		public static RelationshipType manual(long topic) {
																return combineTypeAndId(MANUAL, topic);
																									   }

		public static RelationshipType authored(long topic) {
																  return combineTypeAndId(AUTHORED, topic);
																										   }

		public static RelationshipType connected(long topic) {
																   return combineTypeAndId(CONNECTED, topic);
																											 }

		public static RelationshipType ranked() {
													  return RANKED_TYPE;
																		 }

		private static RelationshipType combineTypeAndId(String s, long id) {
																				  return RelationshipType.withName(s + "_" + id);
																																 }

		private Types() {}
	}

	public final static class Fields {
		public static String RANK = "rank";
	}

	private Relationships() {}
}
