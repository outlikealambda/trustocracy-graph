package outlikealambda.utils;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import outlikealambda.traversal.walk.Navigator;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public final class Traversals {

	public static <T> Stream<T> goStream(Iterable<T> things) {
		return StreamSupport.stream(things.spliterator(), false);
	}

	private Traversals() {}

	public static Function<Node, Relationship> getSingleOut(RelationshipType rt) {
		return getSingle(rt, Direction.OUTGOING);
	}

	public static Function<Node, Relationship> getSingle(RelationshipType rt, Direction d) {
		return n -> n.getSingleRelationship(rt, d);
	}

	public static <T, U> Optional<U> first(T input, Stream<Function<T, U>> transforms) {
		return transforms
				.map(fn -> fn.apply(input))
				.filter(Objects::nonNull)
				.findFirst();
	}

	public static <T, U> Function<T, U> asFunction(Function<T, U> fn) {
		return fn;
	}

	/**
	 * finds the author for a given connected node.
	 *
	 * throws an IllegalArgumentException if node is not connected
	 */
	public static Node follow(Navigator navigator, Node source) {
		if (navigator.isAuthor(source)) {
			return source;
		}

		return Traversals.asFunction(navigator::getConnectionOut)
				.andThen(Relationship::getEndNode)
				.andThen(n -> follow(navigator, n))
				.apply(source);
	}
}

