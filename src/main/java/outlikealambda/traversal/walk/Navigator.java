package outlikealambda.traversal.walk;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import outlikealambda.traversal.Nodes;
import outlikealambda.traversal.Relationships;
import outlikealambda.utils.Composables;
import outlikealambda.utils.Optionals;

import java.util.Optional;
import java.util.stream.Stream;

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
		this.manualType = Relationships.Types.manual(topicId);
		this.authoredType = Relationships.Types.authored(topicId);
		this.rankedType = Relationships.Types.ranked();
		this.connectedType = Relationships.Types.connected(topicId);
	}

	public boolean isConnected(Node n) {
		return n.hasRelationship(connectedType, Direction.OUTGOING);
	}

	public boolean isDisjoint(Node n) {
		return Nodes.Fields.isDisjoint(n);
	}

	public boolean isAuthor(Node n) {
		return n.hasRelationship(authoredType, Direction.OUTGOING);
	}

	public boolean isOpinion(Node n) {
		return n.hasLabel(Nodes.Labels.OPINION);
	}

	public Node getOpinion(Node author) {
		return Optional.of(author)
				.map(Relationships.getSingleOut(authoredType))
				.map(Relationship::getEndNode)
				.orElseThrow(() -> new IllegalArgumentException("Don't call getOpinion unless you're sure you have an author"));
	}

	// TODO: optimize
	// Is there a way to avoid setting/removing a property on each node?
	public void clearConnectionState(Node n) {
		Optionals.ifElse(
				Optional.of(n).map(Relationships.getSingleOut(connectedType)),
				Relationship::delete,
				() -> Nodes.Fields.setDisjoint(n, false)
		);
	}

	public void setConnected(Node source, Node target) {
		source.createRelationshipTo(target, connectedType);
	}

	public void setDisjoint(Node n) {
		Nodes.Fields.setDisjoint(n, true);
	}

	public void setTarget(Node source, Node target) {
		Relationships.clearAndLinkOut(source, target, manualType);
	}

	public void setOpinion(Node author, Node opinion) {
		Relationships.clearAndLinkOut(author, opinion, authoredType);
	}

	public Relationship getConnectionOut(Node n) {
		return Optionals.first(n,
				Stream.of(Relationships.getSingleOut(connectedType), Relationships.getSingleOut(manualType)))
				.orElseThrow(() -> new IllegalArgumentException(
						"getConnectionOut must have a connection"
				));
	}

	public Stream<Relationship> getConnectionsIn(Node n) {
		return Composables.goStream(n.getRelationships(Direction.INCOMING, connectedType));
	}

	public Stream<Relationship> getRankedAndManualOut(Node n) {
		return Composables.goStream(n.getRelationships(Direction.OUTGOING, rankedType, manualType));
	}

	public Stream<Relationship> getRankedAndManualIn(Node n) {
		return Composables.goStream(n.getRelationships(Direction.INCOMING, manualType, rankedType));
	}

	public Stream<Relationship> getWalkableOutgoing(Node n) {
		return Optionals.first(
				n,
				Stream.of(
						Relationships.getSingleOut(authoredType),
						Relationships.getSingleOut(manualType)))
				.map(Stream::of)
				.orElseGet(() -> getRankedByRank(n));
	}

	private Stream<Relationship> getRankedByRank(Node n) {
		return Composables.goStream(n.getRelationships(rankedType, Direction.OUTGOING))
				.sorted(Relationships.rankComparator);
	}
}
