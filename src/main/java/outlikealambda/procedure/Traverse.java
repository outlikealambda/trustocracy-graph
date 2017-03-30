package outlikealambda.procedure;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import outlikealambda.output.Influence;
import outlikealambda.output.TraversalResult;
import outlikealambda.traversal.Nodes;
import outlikealambda.traversal.Relationships;
import outlikealambda.traversal.walk.Navigator;
import outlikealambda.utils.Traversals;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class Traverse {
	@Context
	public GraphDatabaseService gdb;

	@Procedure("measure.influence")
	public Stream<Influence> measureInfluence(
			@Name("userId") long userId,
			@Name("topicId") long topicId
	) {
		Node user = getPerson(userId);
		Navigator navigator = new Navigator(topicId);

		return Stream.of(Traversals.measureInfluence(navigator, user))
				.map(Influence::new);
	}

	@Procedure("friend.author.opinion")
	public Stream<TraversalResult> friendAuthorOpinion(
			@Name("userId") long userId,
			@Name("topicId") long topicId
	) {
		Node user = getPerson(userId);
		Navigator navigator = new Navigator(topicId);

		Map<Node, Relationship> neighborRelationships = navigator.getRankedAndManualOut(user)
				.collect(toMap(
						Relationship::getEndNode,
						Function.identity(),
						(first, second) -> Relationships.Types.ranked().equals(first.getType()) ? first : second
				));

		Map<Node, Node> neighborToAuthor = neighborRelationships.keySet().stream()
				.filter(navigator::isConnected)
				.collect(toMap(
						Function.identity(),
						neighbor -> Traversals.follow(navigator, neighbor)
				));

		Map<Node, Node> authorOpinions = neighborToAuthor.values().stream()
				.distinct()
				.collect(toMap(
						Function.identity(),
						navigator::getOpinion
				));

		Optional<Node> currentTarget = Optional.of(user)
				.filter(navigator::isConnected)
				.map(navigator::getConnectionOut)
				.map(Relationship::getEndNode);

		return TraversalResult.mergeIntoTraversalResults(
				neighborRelationships,
				neighborToAuthor,
				authorOpinions,
				currentTarget
		);
	}

	private Node getPerson(long userId) {
		return gdb.findNode(Nodes.Labels.PERSON, Nodes.Fields.ID, userId);
	}
}
