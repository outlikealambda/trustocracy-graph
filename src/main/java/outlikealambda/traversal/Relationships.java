package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class Relationships {
	private static final String MANUAL = "MANUAL";
	private static final String PROVISIONAL = "PROVISIONAL";
	private static final String AUTHORED = "AUTHORED";
	private static final String RANKED = "RANKED";
	private static final RelationshipType RANKED_TYPE = RelationshipType.withName(RANKED);

	private static class Types {
		private static RelationshipType manual(long topic) {
			return combineTypeAndId(MANUAL, topic);
		}

		private static RelationshipType provisional(long topic) {
			return combineTypeAndId(PROVISIONAL, topic);
		}

		private static RelationshipType authored(long topic) {
			return combineTypeAndId(AUTHORED, topic);
		}

		private static RelationshipType combineTypeAndId(String s, long id) {
			return RelationshipType.withName(s + "_" + id);
		}
	}

	private Relationships() {}

	public static class Topic {
		private final RelationshipType manualType;
		private final RelationshipType provisionalType;
		private final RelationshipType authoredType;

		public Topic(long topic) {
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

		public Iterable<Relationship> getAllIncoming(Node n) {
			return getAll(n, Direction.INCOMING);
		}

		public Iterable<Relationship> getAllOutgoing(Node n) {
			return getAll(n, Direction.OUTGOING);
		}

		public Iterable<Relationship> getTargetedIncoming(Node n) {
			return getTargeted(n, Direction.INCOMING);
		}

		public Optional<Relationship> getTargetedOutgoing(Node n) {
			return atMostOneOf(getTargeted(n, Direction.OUTGOING), "targeted outgoing connection");
		}

		private static <T> Optional<T> atMostOneOf(Iterable<T> things, String description) {
			List<T> list = new ArrayList<>();

			things.forEach(list::add);

			if (list.size() > 1) {
				throw new IllegalStateException(String.format("Can only have a %s per topic", description));
			} else if (list.size() == 1) {
				return Optional.of(list.get(0));
			} else {
				return Optional.empty();
			}
		}

		private Iterable<Relationship> getTargeted(Node n, Direction d) {
			return n.getRelationships(d, getManualType(), getProvisionalType());
		}

		private Iterable<Relationship> getAll(Node n, Direction d) {
			return n.getRelationships(d, getManualType(), getProvisionalType(), getRankedType());
		}
	}
}
