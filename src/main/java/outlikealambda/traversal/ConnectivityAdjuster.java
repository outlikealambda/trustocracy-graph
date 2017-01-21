package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import outlikealambda.utils.Optionals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ConnectivityAdjuster {
	private final RelationshipFilter rf;

	public ConnectivityAdjuster(RelationshipFilter rf) {
		this.rf = rf;
	}

	public Optional<Node> getDesignatedAuthor(Node source) {
		Node current = source;

		while(true) {
			if (current.hasRelationship(rf.getAuthoredType(), Direction.OUTGOING)) {
				// found a connected opinion
				return Optional.of(current);
			}

			Optional<Relationship> outgoing = rf.getTargetedOutgoing(current);

			if (!outgoing.isPresent()) {
				// end of path, with no opinion
				return Optional.empty();
			}

			// continue down the path
			current = outgoing.get().getEndNode();
		}
	}

	public int calculateInfluence(Node target) {
		return TraversalUtils.goStream(rf.getTargetedIncoming(target))
				.map(Relationship::getStartNode)
				.map(this::calculateInfluence)
				.reduce(1, (total, perSourceTotal) -> total + perSourceTotal);
	}

	public void clearTarget(Node source) {
		changeTarget(source, null, null);
	}

	public void setTarget(Node source, Node target) {
		changeTarget(source, target, Collections.singletonList(rf.getManualType()));
	}

	public void setOpinion(Node source, Node opinion) {
		changeTarget(source, opinion, Arrays.asList(rf.getAuthoredType(), rf.getOnceAuthoredType()));
	}

	private void changeTarget(Node source, Node t, List<RelationshipType> relationshipTypes) {
		boolean wasConnected = isConnected(source);

		// Delete the existing outgoing relationship if it exists
		// Could be either manual or provisioned
		rf.getTargetedOutgoing(source)
				.ifPresent(Relationship::delete);

		rf.getAuthored(source)
				.ifPresent(Relationship::delete);


		// create a new relationship to:
		// 1. manually specified node
		// 2. provisioned node
		// 3. nowhere
		Optionals.ifElse(
				Optional.ofNullable(t),

				// create relationships if target exists
				target -> relationshipTypes.forEach(rType -> source.createRelationshipTo(target, rType)),

				// try to create a provisional relationship
				() -> getProvisionalTarget(source)
						.ifPresent(target -> source.createRelationshipTo(target, rf.getProvisionalType()))
		);

		if (clearIfCycle(source)) {
			// cycle here, so clear and bail
			return;
		}

		// flip if necessary
		boolean isConnected = isConnected(source);

		if (wasConnected && !isConnected) {
			rf.getAllIncoming(source).forEach(this::flipLostConnection);
		}

		if (!wasConnected && isConnected) {
			rf.getAllIncoming(source).forEach(this::flipGainedConnection);
		}
	}

	void flipGainedConnection(Relationship incoming) {
		Node source = incoming.getStartNode();

		if (rf.isManual(incoming)) {
			// source's sources could flip, depending on their state
			rf.getAllIncoming(source)
					.forEach(this::flipGainedConnection);

		} else if (rf.isRanked(incoming)) {
			// source has other manual relationship, so won't flip
			if (source.hasRelationship(rf.getManualType(), Direction.OUTGOING)) {
				return;
			}

			if (source.hasRelationship(rf.getProvisionalType(), Direction.OUTGOING)) {
				getProvisionalTarget(source)
						// We only care if the new provisional target is now the same
						// as the ranked incoming relationship
						.filter(incoming.getEndNode()::equals)
						.ifPresent(newSourceTarget -> {

							// delete old provisional relationship
							source.getSingleRelationship(rf.getProvisionalType(), Direction.OUTGOING)
									.delete();

							// create provisional relationship to new target
							source.createRelationshipTo(newSourceTarget, rf.getProvisionalType());

							// check if that creates a cycle
							clearIfCycle(source);
						});

			} else {
				// not possible to create a cycle if node wasn't previously provisioned
				source.createRelationshipTo(incoming.getEndNode(), rf.getProvisionalType());

				// no previous target, so this must flip
				rf.getAllIncoming(source).forEach(this::flipGainedConnection);
			}
		}
	}

	// traverses through incoming connections
	void flipLostConnection(Relationship incoming) {

		Node source = incoming.getStartNode();

		if (rf.isManual(incoming)) {
			// Manual incoming relationship means that sources of that manual source
			// (grandchildren of this target) may flip (depending on their state),
			// so continue traversal
			rf.getTargetedIncoming(source).forEach(this::flipLostConnection);

		} else if (rf.isProvisional(incoming)) {
			// no longer valid, since parent has no connection
			incoming.delete();

			Optionals.ifElse(
					getProvisionalTarget(source),

					// Has a new target, so create that relationship,
					// but also check if new target creates a cycle
					newTarget -> {
						source.createRelationshipTo(newTarget, rf.getProvisionalType());
						clearIfCycle(source);
					},

					// No new target (which means this node has also flipped),
					// so flip all incoming of the source node
					() -> rf.getTargetedIncoming(source).forEach(this::flipLostConnection)
			);
		}
		// case: the incoming relationship is ranked
		// The source currently points elsewhere, or doesn't point anywhere.
		// Nothing to do here
	}

	Optional<Node> getProvisionalTarget(Node n) {
		return TraversalUtils.goStream(n.getRelationships(Direction.OUTGOING, rf.getRankedType()))
				.sorted(RelationshipFilter.rankComparator)
				.map(Relationship::getEndNode)
				.filter(this::isConnected)
				.findFirst();
	}

	boolean isConnected(Node n) {
		if (n.hasRelationship(
				Direction.OUTGOING,
				rf.getAuthoredType(),
				rf.getProvisionalType())) {
			return true;
		}

		return Optional.of(n)
				.map(current -> current.getSingleRelationship(rf.getManualType(), Direction.OUTGOING))
				.map(Relationship::getEndNode)
				.map(this::isConnected)
				.orElse(false);
	}

	boolean clearIfCycle(Node start) {
		Collection<Node> cycle = getCycle(start);

		// great
		if (cycle.isEmpty()) {
			return false;
		}

		clearCyclicProvisions(cycle);

		flipCyclicIncoming(cycle);

		return true;
	}

	// empty collection if no cycle
	// no guaranteed order; switch to LinkedHashSet if that's necessary
	Collection<Node> getCycle(Node node) {
		LinkedHashSet<Node> visited = new LinkedHashSet<>();
		Node current = node;
		visited.add(node);

		while(true) {
			Optional<Node> next = getSelected(current)
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
	private void clearCyclicProvisions(Collection<Node> cycle) {
		cycle.stream()
				.map(cycleNode -> cycleNode.getSingleRelationship(rf.getProvisionalType(), Direction.OUTGOING))
				.filter(Objects::nonNull)
				.forEach(Relationship::delete);
	}

	// If we've created a cycle, we may have lost an endpoint, and
	// so we have to flip any incoming connections which previously
	// routed to that endpoint
	private void flipCyclicIncoming(Collection<Node> cycle) {
		cycle.stream()
				.map(rf::getTargetedIncoming)
				.flatMap(TraversalUtils::goStream)
				// skip any remaining manual relationships between the nodes in the cycle
				.filter(r -> !cycle.contains(r.getStartNode()))
				.forEach(this::flipLostConnection);
	}

	private Optional<Relationship> getSelected(Node n) {
		return Optional.of(n)
				.flatMap(rf::getTargetedOutgoing);
	}
}
