package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class TraversalUtils {

	public static <T> Stream<T> goStream(Iterable<T> things) {
		return StreamSupport.stream(things.spliterator(), false);
	}

	private TraversalUtils() {}

	public static Function<Node, Relationship> getSingleOut(RelationshipType rt) {
		return getSingle(rt, Direction.OUTGOING);
	}

	public static Function<Node, Relationship> getSingle(RelationshipType rt, Direction d) {
		return n -> n.getSingleRelationship(rt, d);
	}
}

