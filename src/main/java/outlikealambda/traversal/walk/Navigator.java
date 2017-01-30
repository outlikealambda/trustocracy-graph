package outlikealambda.traversal.walk;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import outlikealambda.traversal.RelationshipFilter;
import outlikealambda.traversal.RelationshipTypes;
import outlikealambda.utils.Optionals;
import outlikealambda.utils.Traversals;

import java.util.Optional;
import java.util.stream.Stream;

import static outlikealambda.utils.Traversals.getSingleOut;

/**
 * Reads and modifies the connections between nodes in a walk-based
 * graph
 */
public class Navigator {

	private final RelationshipType manualType;
	private final RelationshipType authoredType;
	private final RelationshipType rankedType;
	private final RelationshipType connectedType;

	public Navigator(long topicId) {
		this.manualType = RelationshipTypes.manual(topicId);
		this.authoredType = RelationshipTypes.authored(topicId);
		this.rankedType = RelationshipTypes.ranked();
		this.connectedType = RelationshipTypes.connected(topicId);
	}

	public boolean isConnected(Node n) {
		return n.hasRelationship(connectedType, Direction.OUTGOING);
	}

	public boolean isDisjoint(Node n) {
		return n.hasProperty("disjoint");
	}

	public boolean isAuthor(Node n) {
		return n.hasRelationship(authoredType, Direction.OUTGOING);
	}

	// TODO: optimize
	// Is there a way to avoid setting/removing a property on each node?
	//
	public void cleanState(Node n) {
		Optionals.ifElse(
				Optional.of(n).map(getSingleOut(connectedType)),
				Relationship::delete,
				() -> n.removeProperty("disjoint")
		);
	}

	public void setConnected(Node source, Node target) {
		source.createRelationshipTo(target, connectedType);
	}

	public void setDisjoint(Node n) {
		n.setProperty("disjoint", true);
	}

	public Relationship getConnectionOut(Node n) {
		return Traversals.first(n,
				Stream.of(getSingleOut(connectedType), getSingleOut(manualType)))
				.orElseThrow(() -> new IllegalArgumentException(
						"getConnectionOut must have a connection"
				));
	}

	public Stream<Relationship> getWalkableOutgoing(Node n) {
		return Optional.of(n)
				.map(getSingleOut(manualType))
				.map(Stream::of)
				.orElseGet(() -> getRankedByRank(n));
	}

	public Stream<Relationship> getRankedAndManualIncoming(Node n) {
		return Traversals.goStream(n.getRelationships(Direction.INCOMING, manualType, rankedType));
	}

	private Stream<Relationship> getRankedByRank(Node n) {
		return Traversals.goStream(n.getRelationships(rankedType, Direction.OUTGOING))
				.sorted(RelationshipFilter.rankComparator);
	}


}
