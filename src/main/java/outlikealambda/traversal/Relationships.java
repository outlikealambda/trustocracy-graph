package outlikealambda.traversal;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import static outlikealambda.traversal.Relationships.Types.manual;
import static outlikealambda.traversal.Relationships.Types.ranked;

public final class Relationships {
	private static final String MANUAL = "MANUAL";
	private static final String PROVISIONAL = "PROVISIONAL";
	private static final String AUTHORED = "AUTHORED";
	private static final String RANKED = "RANKED";
	private static final RelationshipType RANKED_TYPE = RelationshipType.withName(RANKED);

	public static class Types {
		public static RelationshipType manual(int topic) {
			return combineTypeAndId(MANUAL, topic);
		}

		public static RelationshipType provisional(int topic) {
			return combineTypeAndId(PROVISIONAL, topic);
		}

		public static RelationshipType authored(int topic) {
			return combineTypeAndId(AUTHORED, topic);
		}

		public static RelationshipType ranked() {
			return RANKED_TYPE;
		}
		private static RelationshipType combineTypeAndId(String s, int id) {
			return RelationshipType.withName(s + "_" + id);
		}
	}

	public static boolean isRanked(Relationship r) {
		return r.isType(ranked());
	}

	public static boolean isManual(Relationship r, int topic) {
		return r.isType(manual(topic));
	}

	private Relationships() {}

	public static class Topic {
		private final RelationshipType manualType;
		private final RelationshipType provisionalType;
		private final RelationshipType authoredType;

		public Topic(int topic) {
			this.manualType = Types.manual(topic);
			this.provisionalType = Types.provisional(topic);
			this.authoredType = Types.authored(topic);
		}

		public boolean isManual(Relationship r) {
			return r.isType(manualType);
		}

		public RelationshipType getManualType() {
			return manualType;
		}

		public boolean isProvisional(Relationship r) {
			return r.isType(provisionalType);
		}

		public RelationshipType getProvisionalType() {
			return provisionalType;
		}

		public RelationshipType getAuthoredType() {
			return authoredType;
		}

		public boolean isRanked(Relationship r) {
			return r.isType(RANKED_TYPE);
		}

		public RelationshipType getRankedType() {
			return RANKED_TYPE;
		}
	}
}
