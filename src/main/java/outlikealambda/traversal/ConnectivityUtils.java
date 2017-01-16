package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public final class ConnectivityUtils {

	// empty collection if no cycle
	// no guaranteed order; switch to LinkedHashSet if that's necessary
	public static Collection<Node> getCycle(Node node, Relationships.Topic topic) {
		LinkedHashSet<Node> visited = new LinkedHashSet<>();
		Node current = node;
		visited.add(node);

		while(true) {
			Optional<Node> next = getSelected(current, topic)
					.map(Relationship::getEndNode);

			// found the top of this trace, so no cycle
			if (!next.isPresent()) {
				return Collections.emptyList();
			}

			current = next.get();

			// we've arrived back at our starting point -- cycle!
			if (current.equals(node)) {
				return visited;
			}

			// we've already visited this non-start node; sub-cycles are ok!
			if (!visited.add(current)) {
				return Collections.emptyList();
			}
		}
	}

	private static Optional<Relationship> getSelected(Node n, Relationships.Topic topic) {
		return Optional.of(n)
				.map(node -> node.getRelationships(
						Direction.OUTGOING,
						topic.getManualType(),
						topic.getProvisionalType())
				)
				.map(TraversalUtils::goStream)
				.flatMap(Stream::findFirst);
	}

	private ConnectivityUtils() {}
}
