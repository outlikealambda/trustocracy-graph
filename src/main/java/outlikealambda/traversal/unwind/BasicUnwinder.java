package outlikealambda.traversal.unwind;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import outlikealambda.traversal.walk.Navigator;

import java.util.LinkedHashSet;
import java.util.LinkedList;

public class BasicUnwinder implements Unwinder {
	private final Navigator nav;

	public BasicUnwinder(Navigator nav) {
		this.nav = nav;
	}

	/**
	 * Breadth first seems reasonable -- leaves the furthest upstream for last.
	 * Collects all upstream nodes, clearing their connection state (connected/disjoint)
	 * as it finds them.
	 */
	@Override
	public LinkedHashSet<Node> unwind(Node start) {
		LinkedHashSet<Node> upstream = new LinkedHashSet<>();
		LinkedList<Node> queue = new LinkedList<>();

		queue.push(start);

		Node current;

		while(!queue.isEmpty()) {
			current = queue.pop();

			// go to next node in queue if we've already visited
			if (!upstream.contains(current)) {

				// remove any connected/disjoint state
				nav.clearConnectionState(current);

				upstream.add(current);

				nav.getRankedAndManualIn(current)
						.map(Relationship::getStartNode)
						.forEach(queue::add);
			}
		}

		return upstream;
	}

}
