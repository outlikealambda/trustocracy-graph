package outlikealambda.procedure;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import outlikealambda.output.TraversalResult;
import outlikealambda.traversal.ConnectivityManager;
import outlikealambda.traversal.walk.Navigator;
import outlikealambda.utils.Traversals;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class CleanConnectivity {
	private static final Label PERSON_LABEL = Label.label("Person");
	private static final String PERSON_ID = "id";

	private static final Label OPINION_LABEL = Label.label("Opinion");
	private static final String OPINION_ID = "id";

	@Context
	public GraphDatabaseService gdb;

	@Procedure("clean.friend.author.opinion")
	public Stream<TraversalResult> traverse(
			@Name("userId") long userId,
			@Name("topicId") long topicId
	) {
		Node user = getPerson(userId);
		Navigator navigator = new Navigator(topicId);

		Map<Node, Relationship> neighborRelationships = navigator.getRankedAndManualOut(user)
				.collect(toMap(
						Relationship::getEndNode,
						Function.identity()
				));

		Map<Node, Node> neighborToAuthor = neighborRelationships.keySet().stream()
				.filter(navigator::isConnected)
				.map(neighbor -> Pair.of(
						neighbor,
						Traversals.follow(navigator, neighbor)
				))
				.collect(toMap(
						Pair::getLeft,
						Pair::getRight
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

	@Procedure("clean.update")
	public void updateConnectivity(
			@Name("userId") long userId,
			@Name("topicId") long topicId
	) {
		ConnectivityManager manager = ConnectivityManager.dirtyWalk(topicId);

		Node user = getPerson(userId);

		manager.updateConnectivity(user);
	}

	@Procedure("clean.target.set")
	@PerformsWrites
	public void setTarget(
			@Name("userId") long userId,
			@Name("targetId") long targetId,
			@Name("topicId") long topicId
	) {
		ConnectivityManager manager = ConnectivityManager.unwindAndWalk(topicId);

		Node user = getPerson(userId);
		Node target = getPerson(targetId);

		manager.setTarget(user, target);
	}

	@Procedure("clean.target.clear")
	@PerformsWrites
	public void clearTarget(
			@Name("userId") long userId,
			@Name("topicId") long topicId
	) {
		ConnectivityManager manager = ConnectivityManager.unwindAndWalk(topicId);

		Node user = getPerson(userId);

		manager.clearTarget(user);
	}

	@Procedure("clean.opinion.set")
	@PerformsWrites
	public void setOpinion(
			@Name("userId") long userId,
			@Name("opinionId") long opinionId,
			@Name("topicId") long topicId
	) {
		ConnectivityManager manager = ConnectivityManager.unwindAndWalk(topicId);

		Node user = getPerson(userId);
		Node opinion = getOpinion(opinionId);

		manager.setOpinion(user, opinion);
	}

	@Procedure("clean.opinion.clear")
	@PerformsWrites
	public void clearOpinion(
			@Name("userId") long userId,
			@Name("topicId") long topicId
	) {
		ConnectivityManager manager = ConnectivityManager.unwindAndWalk(topicId);

		Node user = getPerson(userId);

		manager.clearOpinion(user);
	}

	private Node getPerson(long userId) {
		return gdb.findNode(PERSON_LABEL, PERSON_ID, userId);
	}

	private Node getOpinion(long opinionId) {
		return gdb.findNode(OPINION_LABEL, OPINION_ID, opinionId);
	}
}
