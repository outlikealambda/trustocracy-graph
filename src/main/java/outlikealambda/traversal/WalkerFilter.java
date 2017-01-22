package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import outlikealambda.utils.Optionals;

import java.util.Optional;
import java.util.stream.Stream;

public class WalkerFilter {

	private final RelationshipType manualType;
	private final RelationshipType authoredType;
	private final RelationshipType rankedType;
	private final RelationshipType connectedType;

	public WalkerFilter(long topicId) {
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
				Optional.ofNullable(n.getSingleRelationship(connectedType, Direction.OUTGOING)),
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

	public Stream<Relationship> getWalkableOutgoing(Node n) {
		return Optional.ofNullable(n.getSingleRelationship(manualType, Direction.OUTGOING))
				.map(Stream::of)
				.orElseGet(() -> getRankedByRank(n));
	}

	private Stream<Relationship> getRankedByRank(Node n) {
		return TraversalUtils.goStream(n.getRelationships(rankedType, Direction.OUTGOING))
				.sorted(RelationshipFilter.rankComparator);
	}

	public Stream<Relationship> getRankedAndManualIncoming(Node n) {
		return TraversalUtils.goStream(n.getRelationships(Direction.INCOMING, manualType, rankedType));
	}


}
