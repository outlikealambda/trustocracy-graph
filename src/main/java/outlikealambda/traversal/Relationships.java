package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import outlikealambda.utils.Composables;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

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

	public static Function<Node, Relationship> getSingleOut(RelationshipType rt) {
		return getSingle(rt, Direction.OUTGOING);
	}

	private static Function<Node, Relationship> getSingle(RelationshipType rt, Direction d) {
		return n -> n.getSingleRelationship(rt, d);
	}

	private static Function<Node, Iterable<Relationship>> getAllOut(RelationshipType rt) {
		return n -> n.getRelationships(rt);
	}

	public static void clearAndLinkOut(Node source, Node target, RelationshipType rt) {
		clearRelationshipOut(source, rt);
		setRelationshipOut(source, target, rt);
	}

	public static void setRanked(Node source, List<Node> rankedTargets) {
		// clear all ranked relationships
		getRankedOutgoing(source).forEach(Relationship::delete);

		// build all input ranked relationships
		for (int i = 0; i < rankedTargets.size(); i++) {
			source.createRelationshipTo(rankedTargets.get(i), Types.RANKED_TYPE)
					.setProperty(Fields.RANK, (long) i);
		}
	}

	public static Stream<Relationship> getRankedOutgoing(Node source) {
		return Composables.goStream(source.getRelationships(Types.RANKED_TYPE, Direction.OUTGOING));
	}

	public static Comparator<Relationship> rankComparator =
			(left, right) -> getRank(left) < getRank(right) ? -1 : 1;

	public static long getRank(Relationship r) {
		return (long) r.getProperty(Fields.RANK);
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

	private Relationships() {}
}
