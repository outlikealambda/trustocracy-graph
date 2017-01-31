package outlikealambda.traversal.walk;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import outlikealambda.utils.Optionals;
import outlikealambda.utils.Traversals;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Future improvements:
 *
 * Be more selective when clearing/collecting nodes
 * - Can terminate on authors
 *
 * Store the final destination
 * - No need to traverse on lookups
 *
 * Does this need to happen every time?
 * - maybe only when gaining/losing a connection
 * - or when the initial change (manual) completes a cycle
 *
 */
public class Walker {
	private final Navigator navigator;
	private final HashSet<Node> visited = new HashSet<>();

	public Walker(Navigator navigator) {
		this.navigator = navigator;
	}

	/**
	 * finds the author for a given connected node.
	 *
	 * throws an IllegalArgumentException if node is not connected
	 *
	 * @param source
	 * @return
	 */
	public Node follow(Node source) {
		if (navigator.isAuthor(source)) {
			return source;
		}

		return Traversals.asFunction(navigator::getConnectionOut)
				.andThen(Relationship::getEndNode)
				.andThen(this::follow)
				.apply(source);
	}

	/**
	 * unwinds, and then blazes starting from all unwound nodes
	 */
	public void updateFromNode(Node origin) {
		unwindUpstream(origin)
				.forEach(this::blaze);
	}

	/**
	 * "blazing a path"
	 *
	 * walk a path until the end, updating all unmarked nodes
	 *
	 * @param source
	 * @return
	 */
	WalkResult blaze(Node source) {
		// Already visited
		if (navigator.isConnected(source)) {
			return new WalkResult(true);
		}

		// Already visited
		if (navigator.isDisjoint(source)) {
			return new WalkResult(false);
		}

		// Author
		if (navigator.isAuthor(source)) {
			return new WalkResult(true);
		}

		// We've found a cycle!
		if (visited.contains(source)) {
			return new WalkResult(false, source);
		}

		// Record as visited
		visited.add(source);

		// Walk through the outgoing connections (connection order logic is in WalkerFilter).
		// - if we have a manual outgoing, check _only_ that connection
		// - otherwise, check all ranked outgoing connections (in ranked order)
		//
		// We can stop when:
		// 1. We find a connected child -- that means we're connected
		// 2. A child returns an Optional<Node> cycleMarker.  That means we're part of a cycle
		//    and need to mark ourselves as such and let the previous node know (unless we are
		//    the cycle end)
		// 3. We've checked all our outgoing connections, with no success.  That means we're disjoint
		Optional<Pair<Node, WalkResult>> targetWalkResult = navigator.getWalkableOutgoing(source)
				.map(Relationship::getEndNode)
				.map(target -> Pair.of(target, blaze(target)))
				.filter(pair ->
						pair.getRight().isConnected() || pair.getRight().getCycleEnd().isPresent())
				.findFirst();

		return Optionals.ifElseMap(
				targetWalkResult,
				twr -> {
					// we found a result, either:
					// 1. a connected outgoing target
					// 2. a cycle :(
					Node target = twr.getLeft();
					WalkResult walkResult = twr.getRight();

					if (walkResult.isConnected()) {
						navigator.setConnected(source, target);

						// pass through
						return walkResult;
					} else {
						// We have a non-connected result, that didn't get filtered;
						// must be a cycle
						navigator.setDisjoint(source);

						// IFF it's the cycle source, we've reached the end of the cycle,
						// and can remove it from the WalkResult
						return walkResult.getCycleEnd()
								.filter(source::equals)
								.map(ignored -> new WalkResult(false))
								.orElse(walkResult);
					}

				},
				() -> {
					// all of the outgoing targets were disjoint, so we ourselves
					// are disjoint
					navigator.setDisjoint(source);
					return new WalkResult(false);
				}
		);

	}

	/**
	 * Breadth first seems reasonable -- leaves the furthest upstream for last.
	 * Collects all upstream nodes, clearing their connection state (connected/disjoint)
	 * as it finds them.
	 */
	LinkedHashSet<Node> unwindUpstream(Node start) {
		LinkedHashSet<Node> upstream = new LinkedHashSet<>();
		LinkedList<Node> queue = new LinkedList<>();

		queue.push(start);

		Node current;

		while(!queue.isEmpty()) {
			current = queue.pop();

			// go to next node in queue if we've already visited
			if (!upstream.contains(current)) {

				// remove any connected/disjoint state
				navigator.cleanState(current);

				upstream.add(current);

				navigator.getRankedAndManualIncoming(current)
						.map(Relationship::getStartNode)
						.forEach(queue::add);
			}
		}

		return upstream;
	}

	private class WalkResult {
		private final boolean isConnected;
		private final Node cycleEnd;

		public WalkResult(boolean isConnected) {
			this(isConnected, null);
		}

		public WalkResult(boolean isConnected, Node cycleEnd) {
			this.isConnected = isConnected;
			this.cycleEnd = cycleEnd;
		}

		public boolean isConnected() {
			return isConnected;
		}

		public Optional<Node> getCycleEnd() {
			return Optional.ofNullable(cycleEnd);
		}
	}
}
