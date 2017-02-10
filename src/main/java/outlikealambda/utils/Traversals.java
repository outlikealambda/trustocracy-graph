package outlikealambda.utils;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import outlikealambda.traversal.Relationships;
import outlikealambda.traversal.walk.Navigator;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;


public final class Traversals {

	public static Function<Node, Relationship> getSingleOut(RelationshipType rt) {
		return getSingle(rt, Direction.OUTGOING);
	}

	private static Function<Node, Relationship> getSingle(RelationshipType rt, Direction d) {
		return n -> n.getSingleRelationship(rt, d);
	}

	public static void clearAndLinkOut(Node source, Node target, RelationshipType rt) {
		clearRelationshipOut(source, rt);
		setRelationshipOut(source, target, rt);
	}

	public static void setRanked(Node source, List<Node> rankedTargets) {
		clearRelationshipOut(source, Relationships.Types.ranked());

		for (int i = 0; i < rankedTargets.size(); i++) {
			source.createRelationshipTo(rankedTargets.get(i), Relationships.Types.ranked())
					.setProperty(Relationships.Fields.RANK, i);
		}
	}

	public static Comparator<Relationship> rankComparator =
			(left, right) -> getRank(left) < getRank(right) ? -1 : 1;

	private static long getRank(Relationship r) {
		return (long) r.getProperty(Relationships.Fields.RANK);
	}

	private static void clearRelationshipOut(Node source, RelationshipType rt) {
		Optional.of(source)
				.map(getSingleOut(rt))
				.ifPresent(Relationship::delete);
	}

	private static void setRelationshipOut(Node source, Node target, RelationshipType rt) {
		Optional.ofNullable(target)
				.ifPresent(t -> source.createRelationshipTo(t, rt));
	}

	/**
	 * finds the author for a given connected node.
	 * <p>
	 * throws an IllegalArgumentException if node is not connected
	 */
	public static Node follow(Navigator navigator, Node source) {
		if (navigator.isAuthor(source)) {
			return source;
		}

		return Composables.asFunction(navigator::getConnectionOut)
				.andThen(Relationship::getEndNode)
				.andThen(n -> follow(navigator, n))
				.apply(source);
	}

	private Traversals() {}
}

