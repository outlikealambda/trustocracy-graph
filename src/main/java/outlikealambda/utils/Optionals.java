package outlikealambda.utils;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class Optionals {
	public static <T, U> T ifElseMap(Optional<U> optional, Function<U, T> fn, Supplier<T> supplier) {
		if (optional.isPresent()) {
			return fn.apply(optional.get());
		}

		return supplier.get();
	}

	public static <T> void ifElse(Optional<T> optional, Consumer<T> consumer, Runnable r) {
		if (optional.isPresent()) {
			consumer.accept(optional.get());
		} else {
			r.run();
		}

	}

	public static <T, U> Optional<U> first(T input, Stream<Function<T, U>> transforms) {
		return transforms
				.map(fn -> fn.apply(input))
				.filter(Objects::nonNull)
				.findFirst();
	}


	private Optionals() {}
}
