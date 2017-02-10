package outlikealambda.utils;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Composables {
	public static <T> Predicate<T> or(Predicate<T> isFirst, Predicate<T> isSecond) {
		return input -> isFirst.test(input) || isSecond.test(input);
	}

	public static <T> Predicate<T> not(Predicate<T> isTrue) {
		return input -> !isTrue.test(input);
	}

	public static <T, U> Function<T, U> asFunction(Function<T, U> fn) {
		return fn;
	}

	public static <T> Stream<T> goStream(Iterable<T> things) {
		return StreamSupport.stream(things.spliterator(), false);
	}
}
