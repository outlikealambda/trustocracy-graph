package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static outlikealambda.traversal.Relationships.Types.manual;
import static outlikealambda.traversal.Relationships.Types.provisional;

public final class ConnectivityUtils {

	public static boolean isCycle(Node node, int topic) {
		Set<Node> visited = new HashSet<>();
		Node current = node;
		visited.add(node);

		while(true) {
			Optional<Node> next = getSelected(current, topic)
					.map(Relationship::getEndNode);

			// found the top of this trace, so no cycle
			if (!next.isPresent()) {
				return false;
			}

			current = next.get();

			// we've arrived back at our starting point -- cycle!
			if (current.equals(node)) {
				return true;
			}

			// we've already visited this non-start node; sub-cycles are ok!
			if (!visited.add(current)) {
				return false;
			}
		}
	}

	private static Optional<Relationship> getSelected(Node n, int topic) {
		return Optional.of(n)
				.map(node -> node.getRelationships(Direction.OUTGOING, manual(topic), provisional(topic)))
				.map(TraversalUtils::goStream)
				.flatMap(Stream::findFirst);
	}

	private ConnectivityUtils() {}
}
