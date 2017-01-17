package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import outlikealambda.utils.Optionals;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public final class ConnectivityUtils {

	// traverses through incoming connections
	public static void flipLostConnection(Relationship incoming, Relationships.Topic topic) {

		// no work here -- will not flip child nodes currently pointing elsewhere
		if (topic.isRanked(incoming)) {
			return;
		}

		Node child = incoming.getStartNode();

		// manual incoming relationship means that children of that manual node
		// may flip (depending on their state), so continue traversal
		if (topic.isManual(incoming)) {
			topic.getTargetedIncoming(child)
					.forEach(r -> flipLostConnection(r, topic));

			return;
		}

		if (topic.isProvisional(incoming)) {

			// no longer valid, since parent has no connection
			incoming.delete();

			Optionals.ifElse(
					getProvisionalTarget(child, topic),

					// Has a new target, so create that relationship,
					// but also check if new target creates a cycle
					newTarget -> {
						child.createRelationshipTo(newTarget, topic.getProvisionalType());
						cycleCheck(child, topic);
					},

					// No new target (which means this node has also flipped),
					// so flip all incoming of the child node
					() -> topic.getTargetedIncoming(child)
							.forEach(r -> flipLostConnection(r, topic))
			);
		}
	}

	public static Optional<Node> getProvisionalTarget(Node n, Relationships.Topic topic) {
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

	public static void cycleCheck(Node start, Relationships.Topic topic) {
		Collection<Node> cycle = getCycle(start, topic);

		// great
		if (cycle.isEmpty()) {
			return;
		}

		// Delete all provisional relationships in this cycle,
		// because a cycle has no endpoint...
		cycle.stream()
				.map(cycleNode -> cycleNode.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING))
				.filter(Objects::nonNull)
				.forEach(Relationship::delete);

		// Since we've created a cycle, we have lost an endpoint, and
		// so we have to flip any incoming connections which previously
		// routed to that endpoint
		cycle.stream()
				.map(topic::getTargetedIncoming)
				.flatMap(TraversalUtils::goStream)
				// skip any remaining manual relationships between the nodes in the cycle
				.filter(r -> !cycle.contains(r.getStartNode()))
				.forEach(incoming -> flipLostConnection(incoming, topic));
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
				.map(topic::getTargetedOutgoing)
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
