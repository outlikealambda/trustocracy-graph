package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class RelationshipFilter {
	private static final String MANUAL = "MANUAL";
	private static final String PROVISIONAL = "PROVISIONAL";
	private static final String AUTHORED = "AUTHORED";
	private static final String ONCE_AUTHORED = "ONCE_AUTHORED";
	private static final String RANKED = "RANKED";
	private static final RelationshipType RANKED_TYPE = RelationshipType.withName(RANKED);

	private static class Type {
		private static RelationshipType manual(long topic) {
			return combineTypeAndId(MANUAL, topic);
		}

		private static RelationshipType provisional(long topic) {
			return combineTypeAndId(PROVISIONAL, topic);
		}

		private static RelationshipType authored(long topic) {
			return combineTypeAndId(AUTHORED, topic);
		}

		private static RelationshipType onceAuthored(long topic) {
			return combineTypeAndId(ONCE_AUTHORED, topic);
		}

		private static RelationshipType combineTypeAndId(String s, long id) {
			return RelationshipType.withName(s + "_" + id);
		}
	}

	private final RelationshipType manualType;
	private final RelationshipType provisionalType;
	private final RelationshipType authoredType;
	private final RelationshipType onceAuthoredType;

	public RelationshipFilter(long topic) {
		this.manualType = Type.manual(topic);
		this.provisionalType = Type.provisional(topic);
		this.authoredType = Type.authored(topic);
		this.onceAuthoredType = Type.onceAuthored(topic);
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

	// for keeping track of older opinions
	public RelationshipType getOnceAuthoredType() {
		return onceAuthoredType;
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

	public Optional<Relationship> getAuthored(Node n) {
		return atMostOneOf(n.getRelationships(Direction.OUTGOING, getAuthoredType()), "authored relationship");
	}

	public static Comparator<Relationship> rankComparator =
			(left, right) -> getRank(left) < getRank(right) ? -1 : 1;

	private static long getRank(Relationship r) {
		return (long) r.getProperty("rank");
	}

	private static <T> Optional<T> atMostOneOf(Iterable<T> things, String description) {
		List<T> list = new ArrayList<>();

		things.forEach(list::add);

		if (list.size() > 1) {
			throw new IllegalStateException(String.format("Can only have one %s per topic", description));
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
