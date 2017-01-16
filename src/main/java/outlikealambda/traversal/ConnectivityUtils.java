package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.stream.Stream;

public final class ConnectivityUtils {

	public static Optional<Node> getProvisionTarget(Node n, Relationships.Topic topic) {
		return TraversalUtils.goStream(n.getRelationships(Direction.OUTGOING, topic.getRankedType()))
				.sorted(rankComparator)
				.map(Relationship::getEndNode)
				.filter(parent -> isConnected(parent, topic))
				.findFirst();
	}

	public static boolean isConnected(Node n, Relationships.Topic topic) {
		if (n.hasRelationship(
				Direction.OUTGOING,
				topic.getAuthoredType(),
				topic.getProvisionalType())) {
			return true;
		}

		return Optional.of(n)
				.map(current -> current.getSingleRelationship(topic.getManualType(), Direction.OUTGOING))
				.map(Relationship::getEndNode)
				.map(parent -> isConnected(parent, topic))
				.orElse(false);
	}

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

	public static Comparator<Relationship> rankComparator =
			(left, right) -> getRank(left) < getRank(right) ? -1 : 1;

	private static long getRank(Relationship r) {
		return (long) r.getProperty("rank");
	}
}
