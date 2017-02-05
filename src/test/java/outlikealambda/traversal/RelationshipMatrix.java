package outlikealambda.traversal;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class RelationshipMatrix {
	private static boolean[][] init(int size) {
		return new boolean[size][size];
	}

	private static List<Integer> outgoing(int source, boolean[][] m) {
		boolean[] sourceRow = m[source];

		List<Integer> outgoing = new ArrayList<>();

		for (int i = 0; i < sourceRow.length; i++) {
			if (sourceRow[i]) {
				outgoing.add(i);
			}
		}

		return outgoing;
	}

	private static List<Integer> incoming(int target, boolean[][] m) {
		List<Integer> incoming = new ArrayList<>();

		for (int i = 0; i < m.length; i++) {
			if(m[i][target]) {
				incoming.add(i);
			}
		}

		return incoming;
	}

	private static boolean[][] connect(int source, int target, boolean[][] m) {
		m[source][target] = true;

		return m;
	}

	static boolean[][] build(int size) {
		boolean[][] matrix = init(size);

		TargetGenerator targetGenerator = new TargetGenerator(1, .92, 0.92, 0.3, size);

		for(int i = 0; i < size; i++) {
			int source = i;

			targetGenerator.buildOutgoing(source, incoming(source, matrix))
					.forEach(target -> connect(source, target, matrix));
		}

		return matrix;
	}

	static List<Triple<Integer, Integer, Integer>> toDirectedTriples(boolean[][] m) {
		return IntStream.range(0, m.length)
				.mapToObj(source -> Pair.of(source, outgoing(source, m)))
				.peek(pair -> Collections.shuffle(pair.getRight()))
				.flatMap(RelationshipMatrix::rankConnections)
				.collect(toList());
	}

	private static class TargetGenerator {
		private final double ltOne;
		private final double ltThree;
		private final double gteThree;
		private final double reciprocal;
		private final int nodeCount;

		private TargetGenerator(double ltOne, double ltThree, double gteThree, double reciprocal, int nodeCount) {
			this.ltOne = ltOne;
			this.ltThree = ltThree;
			this.gteThree = gteThree;
			this.reciprocal = reciprocal;
			this.nodeCount = nodeCount;
		}

		private List<Integer> buildOutgoing(int source, List<Integer> incoming) {
			List<Integer> outgoing = pickReciprocal(incoming);

			while(shouldAddTarget(outgoing.size())) {
				addId(source, outgoing);
			}

			return outgoing;
		}

		private List<Integer> pickReciprocal(List<Integer> incoming) {
			return incoming.stream()
					.filter(ignored -> happens(reciprocal))
					.collect(toList());
		}

		private boolean shouldAddTarget(int existingCount) {
			if (existingCount < 1) {
				return happens(ltOne);
			}

			if (existingCount < 3) {
				return happens(ltThree);
			}

			return happens(Math.pow(gteThree, existingCount - 2));
		}

		private boolean happens(double withProbability) {
			return ThreadLocalRandom.current().nextDouble() < withProbability;
		}

		private List<Integer> addId(int source, List<Integer> existing) {
			int id;

			do {
				id = ThreadLocalRandom.current().nextInt(nodeCount);
			} while(existing.contains(id) || source == id);

			existing.add(id);

			return existing;
		}
	}

	private static Stream<Triple<Integer, Integer, Integer>> rankConnections(Pair<Integer, List<Integer>> outgoing) {
		final int source = outgoing.getLeft();
		final List<Integer> targets = outgoing.getRight();

		return IntStream.range(0, targets.size())
				.mapToObj(idx -> Triple.of(source, idx, targets.get(idx)));
	}
}
