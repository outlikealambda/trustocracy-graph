package outlikealambda.traversal;

import org.neo4j.graphdb.RelationshipType;

public final class RelationshipTypes {
	private static final String MANUAL = "MANUAL";
	private static final String PROVISIONAL = "PROVISIONAL";
	private static final String AUTHORED = "AUTHORED";
	private static final String ONCE_AUTHORED = "ONCE_AUTHORED";
	private static final String RANKED = "RANKED";
	private static final RelationshipType RANKED_TYPE = RelationshipType.withName(RANKED);

	static RelationshipType manual(long topic) {
		return combineTypeAndId(MANUAL, topic);
	}

	static RelationshipType provisional(long topic) {
		return combineTypeAndId(PROVISIONAL, topic);
	}

	static RelationshipType authored(long topic) {
		return combineTypeAndId(AUTHORED, topic);
	}

	static RelationshipType onceAuthored(long topic) {
		return combineTypeAndId(ONCE_AUTHORED, topic);
	}

	static RelationshipType ranked() {
		return RANKED_TYPE;
	}

	private static RelationshipType combineTypeAndId(String s, long id) {
		return RelationshipType.withName(s + "_" + id);
	}

	private RelationshipTypes() {}
}
