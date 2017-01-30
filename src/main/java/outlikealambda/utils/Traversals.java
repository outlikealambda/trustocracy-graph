package outlikealambda.utils;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

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
}

