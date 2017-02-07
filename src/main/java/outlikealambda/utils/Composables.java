package outlikealambda.utils;

import java.util.function.Predicate;

public class Composables {
	public static <T> Predicate<T> or(Predicate<T> isFirst, Predicate<T> isSecond) {
		return input -> isFirst.test(input) || isSecond.test(input);
	}

	public static <T> Predicate<T> not(Predicate<T> isTrue) {
		return input -> !isTrue.test(input);
	}
}
