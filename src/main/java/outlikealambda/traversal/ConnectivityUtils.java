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

public final class ConnectivityUtils {
	public static Optional<Node> getDesignatedAuthor(Node source, RelationshipFilter.Topic topic) {
		Node current = source;

		while(true) {
			if (current.hasRelationship(topic.getAuthoredType(), Direction.OUTGOING)) {
				// found a connected opinion
				return Optional.of(current);
			}

			Optional<Relationship> outgoing = topic.getTargetedOutgoing(current);

			if (!outgoing.isPresent()) {
				// end of path, with no opinion
				return Optional.empty();
			}

			// continue down the path
			current = outgoing.get().getEndNode();
		}
	}

	public static int calculateInfluence(Node target, RelationshipFilter.Topic topic) {
		return TraversalUtils.goStream(topic.getTargetedIncoming(target))
				.map(Relationship::getStartNode)
				.map(source -> calculateInfluence(source, topic))
				.reduce(1, (total, perSourceTotal) -> total + perSourceTotal);
	}

	public static void clearTarget(Node source, RelationshipFilter.Topic topic) {
		changeTarget(source, null, topic);
	}

	public static void setTarget(Node source, Node target, RelationshipFilter.Topic topic) {
		changeTarget(source, target, topic);
	}

	private static void changeTarget(Node source, Node t, RelationshipFilter.Topic topic) {
		boolean wasConnected = isConnected(source, topic);

		// Delete the existing outgoing relationship if it exists
		// Could be either manual or provisioned
		topic.getTargetedOutgoing(source)
				.ifPresent(Relationship::delete);

		// create a new relationship to:
		// 1. manually specified node
		// 2. provisioned node
		// 3. nowhere
		Optionals.ifElse(
				Optional.ofNullable(t),

				// create manual if parameter exists
				target -> source.createRelationshipTo(target, topic.getManualType()),

				// try to create a provisional relationship
				() -> getProvisionalTarget(source, topic)
						.ifPresent(target -> source.createRelationshipTo(target, topic.getProvisionalType()))
		);

		if (clearIfCycle(source, topic)) {
			// cycle here, so clear and bail
			return;
		}

		// flip if necessary
		boolean isConnected = isConnected(source, topic);

		if (wasConnected && !isConnected) {
			topic.getAllIncoming(source).forEach(r -> flipLostConnection(r, topic));
		}

		if (!wasConnected && isConnected) {
			topic.getAllIncoming(source).forEach(r -> flipGainedConnection(r, topic));
		}
	}

	public static void flipGainedConnection(Relationship incoming, RelationshipFilter.Topic topic) {
		Node source = incoming.getStartNode();

		if (topic.isManual(incoming)) {
			// source's sources could flip, depending on their state
			topic.getAllIncoming(source)
					.forEach(r -> flipGainedConnection(r, topic));

		} else if (topic.isRanked(incoming)) {
			// source has other manual relationship, so won't flip
			if (source.hasRelationship(topic.getManualType(), Direction.OUTGOING)) {
				return;
			}

			if (source.hasRelationship(topic.getProvisionalType(), Direction.OUTGOING)) {
				getProvisionalTarget(source, topic)
						// We only care if the new provisional target is now the same
						// as the ranked incoming relationship
						.filter(incoming.getEndNode()::equals)
						.ifPresent(newSourceTarget -> {

							// delete old provisional relationship
							source.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING)
									.delete();

							// create provisional relationship to new target
							source.createRelationshipTo(newSourceTarget, topic.getProvisionalType());

							// check if that creates a cycle
							clearIfCycle(source, topic);
						});

			} else {
				// not possible to create a cycle if node wasn't previously provisioned
				source.createRelationshipTo(incoming.getEndNode(), topic.getProvisionalType());

				// no previous target, so this must flip
				topic.getAllIncoming(source)
						.forEach(r -> flipGainedConnection(r, topic));
			}
		}
	}

	// traverses through incoming connections
	public static void flipLostConnection(Relationship incoming, RelationshipFilter.Topic topic) {

		Node source = incoming.getStartNode();

		if (topic.isManual(incoming)) {
			// Manual incoming relationship means that sources of that manual source
			// (grandchildren of this target) may flip (depending on their state),
			// so continue traversal
			topic.getTargetedIncoming(source)
					.forEach(r -> flipLostConnection(r, topic));

		} else if (topic.isProvisional(incoming)) {
			// no longer valid, since parent has no connection
			incoming.delete();

			Optionals.ifElse(
					getProvisionalTarget(source, topic),

					// Has a new target, so create that relationship,
					// but also check if new target creates a cycle
					newTarget -> {
						source.createRelationshipTo(newTarget, topic.getProvisionalType());
						clearIfCycle(source, topic);
					},

					// No new target (which means this node has also flipped),
					// so flip all incoming of the source node
					() -> topic.getTargetedIncoming(source)
							.forEach(r -> flipLostConnection(r, topic))
			);
		}
		// case: the incoming relationship is ranked
		// The source currently points elsewhere, or doesn't point anywhere.
		// Nothing to do here
	}

	public static Optional<Node> getProvisionalTarget(Node n, RelationshipFilter.Topic topic) {
		return TraversalUtils.goStream(n.getRelationships(Direction.OUTGOING, topic.getRankedType()))
				.sorted(rankComparator)
				.map(Relationship::getEndNode)
				.filter(target -> isConnected(target, topic))
				.findFirst();
	}

	public static boolean isConnected(Node n, RelationshipFilter.Topic topic) {
		if (n.hasRelationship(
				Direction.OUTGOING,
				topic.getAuthoredType(),
				topic.getProvisionalType())) {
			return true;
		}

		return Optional.of(n)
				.map(current -> current.getSingleRelationship(topic.getManualType(), Direction.OUTGOING))
				.map(Relationship::getEndNode)
				.map(target -> isConnected(target, topic))
				.orElse(false);
	}

	public static boolean clearIfCycle(Node start, RelationshipFilter.Topic topic) {
		Collection<Node> cycle = getCycle(start, topic);

		// great
		if (cycle.isEmpty()) {
			return false;
		}

		clearCyclicProvisions(cycle, topic);

		flipCyclicIncoming(cycle, topic);

		return true;
	}

	// empty collection if no cycle
	// no guaranteed order; switch to LinkedHashSet if that's necessary
	public static Collection<Node> getCycle(Node node, RelationshipFilter.Topic topic) {
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

	// Delete all provisional relationships in this cycle,
	// because a cycle has no endpoint...
	private static void clearCyclicProvisions(Collection<Node> cycle, RelationshipFilter.Topic topic) {
		cycle.stream()
				.map(cycleNode -> cycleNode.getSingleRelationship(topic.getProvisionalType(), Direction.OUTGOING))
				.filter(Objects::nonNull)
				.forEach(Relationship::delete);
	}

	// If we've created a cycle, we may have lost an endpoint, and
	// so we have to flip any incoming connections which previously
	// routed to that endpoint
	private static void flipCyclicIncoming(Collection<Node> cycle, RelationshipFilter.Topic topic) {
		cycle.stream()
				.map(topic::getTargetedIncoming)
				.flatMap(TraversalUtils::goStream)
				// skip any remaining manual relationships between the nodes in the cycle
				.filter(r -> !cycle.contains(r.getStartNode()))
				.forEach(incoming -> flipLostConnection(incoming, topic));

	}

	private static Optional<Relationship> getSelected(Node n, RelationshipFilter.Topic topic) {
		return Optional.of(n)
				.flatMap(topic::getTargetedOutgoing);
	}

	private ConnectivityUtils() {}

	public static Comparator<Relationship> rankComparator =
			(left, right) -> getRank(left) < getRank(right) ? -1 : 1;

	private static long getRank(Relationship r) {
		return (long) r.getProperty("rank");
	}
}
