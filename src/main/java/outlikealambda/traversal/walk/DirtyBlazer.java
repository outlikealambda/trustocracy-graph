package outlikealambda.traversal.walk;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;

/**
 * Basic algorithm:
 *
 * A. (Once) Add modified node to queue (where node target was added/changed/removed)
 *
 * 1. Pull first node off queue
 * 2. Walk from node until: end (opinion, no connection), cycle, or already visited
 * 3. Mark all touched nodes as visited
 * 4. If any touched node changes, add _children_ of touched node to forward walk queue
 * 5. Repeat
 *
 */
public class DirtyBlazer implements Blazer.Controller {
	// Nodes which have already been blazed
	private final Set<Long> processed = new HashSet<>();

	// Nodes to blaze (if not in processed)
	private final LinkedList<Node> queue = new LinkedList<>();

	private final Navigator nav;

	public DirtyBlazer(Navigator nav) {
		this.nav = nav;
	}

	@Override
	public Optional<Blazer.Result> handleProcessed(Node source) {
		return Optional.of(source)
				.filter(n -> processed.contains(n.getId()))
				.map(nav::isConnected)
				.map(Blazer.Result::new);
	}

	@Override
	public Blazer.NodeWalker decorate(Blazer.NodeWalker delegate) {
		return flippedNodeRecorder(delegate);
	}

	private Blazer.NodeWalker flippedNodeRecorder(Blazer.NodeWalker delegate) {
		return node -> {
			boolean before = nav.isConnected(node);

			Blazer.Result result = delegate.walk(node);

			boolean after = nav.isConnected(node);

			// mark the node as processed
			processed.add(node.getId());

			if (before != after) {
				// queue up the incoming relationships of each flipped node
				nav.getRankedAndManualIn(node)
						.map(Relationship::getStartNode)
						.forEach(queue::add);
			}

			return result;
		};
	}

	@Override
	public void go(Node start) {
		Blazer blazer = new Blazer(nav, this);
		processed.clear();
		queue.clear();

		queue.push(start);

		while (!queue.isEmpty()) {
			Node current = queue.poll();

			if (!processed.contains(current.getId())) {
				blazer.start(current);
			}
		}
	}
}

