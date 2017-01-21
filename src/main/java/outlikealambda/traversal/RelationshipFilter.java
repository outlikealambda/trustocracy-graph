package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static outlikealambda.traversal.RelationshipTypes.authored;
import static outlikealambda.traversal.RelationshipTypes.manual;
import static outlikealambda.traversal.RelationshipTypes.onceAuthored;
import static outlikealambda.traversal.RelationshipTypes.provisional;
import static outlikealambda.traversal.RelationshipTypes.ranked;

public class RelationshipFilter {

	private final RelationshipType manualType;
	private final RelationshipType provisionalType;
	private final RelationshipType authoredType;
	private final RelationshipType onceAuthoredType;
	private final RelationshipType rankedType;

	public RelationshipFilter(long topic) {
		this.manualType = manual(topic);
		this.provisionalType = provisional(topic);
		this.authoredType = authored(topic);
		this.onceAuthoredType = onceAuthored(topic);
		this.rankedType = ranked();
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
										  return r.isType(rankedType);
																	   }

	public RelationshipType getRankedType() {
										  return rankedType;
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
