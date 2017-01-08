package outlikealambda.traversal;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class TraversalUtils {

	public static <T> Stream<T> goStream(Iterable<T> things) {
		return StreamSupport.stream(things.spliterator(), false);
	}

	private TraversalUtils() {}
}

