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
import static outlikealambda.utils.Traversals.goStream;

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

	public Node getOpinion(Node author) {
		return Optional.of(author)
				.map(getSingleOut(authoredType))
				.map(Relationship::getEndNode)
				.orElseThrow(() -> new IllegalArgumentException("Don't call getOpinion unless you're sure you have an author"));
	}

	// TODO: optimize
	// Is there a way to avoid setting/removing a property on each node?
	//
	public void clearConnectionState(Node n) {
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

	public void setTarget(Node source, Node target) {
		clearAndLinkOut(source, target, manualType);
	}

	public void setOpinion(Node author, Node opinion) {
		clearAndLinkOut(author, opinion, authoredType);
	}

	private void clearAndLinkOut(Node source, Node target, RelationshipType rt) {
		clearRelationshipOut(source, rt);
		setRelationshipOut(source, target, rt);
	}

	private void clearRelationshipOut(Node source, RelationshipType rt) {
		Optional.of(source)
				.map(getSingleOut(rt))
				.ifPresent(Relationship::delete);
	}

	private void setRelationshipOut(Node source, Node target, RelationshipType rt) {
		Optional.ofNullable(target)
				.ifPresent(t -> source.createRelationshipTo(t, rt));
	}

	public Relationship getConnectionOut(Node n) {
		return Traversals.first(n,
				Stream.of(getSingleOut(connectedType), getSingleOut(manualType)))
				.orElseThrow(() -> new IllegalArgumentException(
						"getConnectionOut must have a connection"
				));
	}

	public Stream<Relationship> getRankedAndManualOut(Node n) {
		return goStream(n.getRelationships(Direction.OUTGOING, rankedType, manualType));
	}

	public Stream<Relationship> getWalkableOutgoing(Node n) {
		return Optional.of(n)
				.map(getSingleOut(manualType))
				.map(Stream::of)
				.orElseGet(() -> getRankedByRank(n));
	}

	public Stream<Relationship> getRankedAndManualIn(Node n) {
		return goStream(n.getRelationships(Direction.INCOMING, manualType, rankedType));
	}

	private Stream<Relationship> getRankedByRank(Node n) {
		return goStream(n.getRelationships(rankedType, Direction.OUTGOING))
				.sorted(RelationshipFilter.rankComparator);
	}


}
