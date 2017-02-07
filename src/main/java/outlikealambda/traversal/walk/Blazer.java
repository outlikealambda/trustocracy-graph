package outlikealambda.traversal.walk;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import outlikealambda.utils.Optionals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class Blazer {
	private final Navigator navigator;
	private final Set<Long> visited = new HashSet<>();
	private Controller controller;

	public Blazer(
			Navigator navigator,
			Controller controller
	) {
		this.navigator = navigator;
		this.controller = controller;
	}

	interface Controller {
		NodeWalker decorate(NodeWalker delegate);

		Optional<Result> handleProcessed(Node node);

		void go(Node start);
	}

	interface NodeWalker {
		Result walk(Node n);
	}

	Collection<Long> start(Node start) {
		visited.clear();

		controller.decorate(this::blaze).walk(start);

		return visited;
	}

	private Result blaze(Node source) {
		Optional<Result> alreadyProcessed = controller.handleProcessed(source);

		if (alreadyProcessed.isPresent()) {
			// we've already processed this node
			return alreadyProcessed.get();
		}

		// We've found a cycle!
		if (!visited.add(source.getId())) {
			return Result.cycle(source.getId());
		}

		// Author
		if (navigator.isAuthor(source)) {
			return Result.pathFound();
		}

		return checkOutgoing(source);
	}

	/**
	 * Walk through the outgoing connections (connection order logic is in Navigator).
	 * - if we have a manual outgoing, check _only_ that connection
	 * - otherwise, check all ranked outgoing connections (in ranked order)
	 *
	 * We can stop when:
	 * 1. We find a connected child -- that means we're connected
	 * 2. A child returns an Optional<Node> cycleMarker.  That means we're part of a cycle
	 *    and need to mark ourselves as such and let the previous node know (unless we are
	 *    the cycle end)
	 * 3. We've checked all our outgoing connections, with no success.  That means we're disjoint
	 */
	private Result checkOutgoing(Node source) {
		Optional<Pair<Node, Result>> targetResult = navigator.getWalkableOutgoing(source)
				.map(Relationship::getEndNode)
				.map(target -> Pair.of(target, controller.decorate(this::blaze).walk(target)))
				.filter(pair -> pair.getRight().isResolved())
				.findFirst();

		return Optionals.ifElseMap(
				targetResult,
				tr -> {
					// we found a result, either:
					// 1. a connected outgoing target
					// 2. a cycle :(
					Node target = tr.getLeft();
					Result result = tr.getRight();

					if (result.isSuccess()) {
						navigator.clearConnectionState(source);
						navigator.setConnected(source, target);

						// pass through
						return result;
					} else {
						// We have a non-connected result, that didn't get filtered;
						// must be a cycle
						navigator.clearConnectionState(source);
						navigator.setDisjoint(source);

						if (result.getCycleEndId() == source.getId()) {
							// If this is the origin of the cycle, simply return a noPathFound,
							// so the upstream node can look for a different path
							return Result.noPathFound();
						} else {
							// Pass through the Result with the cycleEndId so the next upstream
							// node knows that it is part of a cycle
							return result;
						}
					}

				},
				() -> {
					// all of the outgoing targets were disjoint, so we ourselves
					// are disjoint
					navigator.clearConnectionState(source);
					navigator.setDisjoint(source);
					return Result.noPathFound();
				}
		);
	}

	public static class Result {
		private boolean success;
		private Long cycleEndId;

		public Result(boolean success) {
			this.success = success;
		}

		private Result(long cycleEndId) {
			this.success = false;
			this.cycleEndId = cycleEndId;
		}

		private boolean isResolved() {
			return success || cycleEndId != null;
		}

		private boolean isSuccess() {
			return success;
		}

		private long getCycleEndId() {
			return cycleEndId;
		}

		private static Result cycle(long cycleEndId) {
			return new Result(cycleEndId);
		}

		private static Result pathFound() {
			return new Result(true);
		}

		private static Result noPathFound() {
			return new Result(false);
		}
	}
}
