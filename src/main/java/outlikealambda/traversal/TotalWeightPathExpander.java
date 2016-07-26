package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.logging.Log;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TotalWeightPathExpander implements PathExpander<Double> {
	private final Double maxDistance;
	private final long topicId;
	private final Direction expansionDirection;
	private final Log log;

	public TotalWeightPathExpander(Double maxDistance, long topicId, Direction expansionDirection, Log log) {
		this.maxDistance = maxDistance;
		this.topicId = topicId;
		this.expansionDirection = expansionDirection;
		this.log = log;
	}

	@Override public Iterable<Relationship> expand(Path path, BranchState<Double> state) {
//		log.info(String.format("examining %s %s",
//				path.endNode().getLabels().iterator().next().name(),
//				Optional.of(path.endNode())
//					.map(node -> node.getProperty("name"))
//					.orElse("un-named")
//		));

		Double hopCost = Optional.of(path)
				.map(Path::lastRelationship)
				.map(RelationshipLabel::getCost)
				.orElse(0d);

//		log.info(String.format("hop cost: %f", hopCost));

		Double totalCost = hopCost + state.getState();

//		log.info(String.format("total cost: %f", totalCost));

		state.setState(totalCost);

		return StreamSupport.stream(path.endNode().getRelationships(expansionDirection).spliterator(), false)
				.filter(RelationshipLabel.isInteresting(topicId))
				.filter(r -> RelationshipLabel.getCost(r) + totalCost < maxDistance)
				.collect(Collectors.toList());
	}

	@Override public PathExpander<Double> reverse() {
		return null;
	}
}
