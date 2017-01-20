package outlikealambda.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import outlikealambda.model.TraversalResult;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.function.Predicates.not;
import static outlikealambda.traversal.ConnectivityUtils.rankComparator;
import static outlikealambda.traversal.TraversalUtils.goStream;

public class FirstPathDepthFirstTraversal {
	private static final Label PERSON_LABEL = Label.label("Person");
	private static final Label TOPIC_LABEL = Label.label("Topic");
	private static final Label OPINION_LABEL = Label.label("Opinion");

	private static final RelationshipType TRUSTS = RelationshipType.withName("TRUSTS");
	private static final RelationshipType ADDRESSES = RelationshipType.withName("ADDRESSES");
	private static final RelationshipType OPINES = RelationshipType.withName("OPINES");

	@Context
	public GraphDatabaseService db;

	@Context
	public Log log;

	@Procedure("traverse.first")
	public Stream<TraversalResult> traverse(
			@Name("userId") long personId,
			@Name("topicId") long topicId
	) {
		log.info("HERE");

		RelationshipType topicType = RelationshipType.withName("TOPIC_" + String.valueOf(topicId));

		Node topic = db.findNode(TOPIC_LABEL, "id", topicId);

		Map<Node, Node> authorOpinions = goStream(topic.getRelationships(Direction.INCOMING, ADDRESSES))
				.map(Relationship::getStartNode)
				.map(opinion -> opinion.getRelationships(Direction.INCOMING, OPINES))
				.flatMap(TraversalUtils::goStream)
				.collect(toMap(Relationship::getStartNode, Relationship::getEndNode));

		Node start = db.findNode(PERSON_LABEL, "id", personId);

		Iterable<Relationship> userOutgoing = start.getRelationships(Direction.OUTGOING, TRUSTS, topicType);

		Map<Node, Relationship> friendLinks = goStream(userOutgoing)
				.collect(toMap(
						Relationship::getEndNode,
						Function.identity(),
						BinaryOperator.minBy(sortTopicFirstThenRank(topicType))
				));

		Map<Node, Node> friendAuthorNodes = goStream(userOutgoing)
				.map(Relationship::getEndNode)
				.map(n -> {
					DFTraversal dft = new DFTraversal();
					dft.containsAndAdd(start);
					dft.containsAndAdd(n);

					return new PathFinder(topicType, authorOpinions::containsKey).goDeep(n, dft);
				})
				.map(DFTraversal::getPath)
				.filter(not(List::isEmpty))
				.collect(toMap(LinkedList::getFirst, LinkedList::getLast));

		return TraversalResult.mergeIntoTraversalResults(friendLinks, friendAuthorNodes, authorOpinions);
	}

	private static class DFTraversal {
		private Set<Node> visited = new HashSet<>();
		private LinkedList<Node> path = new LinkedList<>();
		private boolean completed = false;

		private boolean containsAndAdd(Node n) {
			return !visited.add(n);
		}

		private void addToPath(Node n) {
			path.add(n);
		}

		private Node popFromPath() {
			return path.pop();
		}

		private boolean isCompleted() {
			return completed;
		}

		private void complete() {
			completed = true;
		}

		private LinkedList<Node> getPath() {
			return path;
		}
	}

	private class PathFinder {
		private final RelationshipType topicType;
		private final Predicate<Node> isFinished;

		private PathFinder(RelationshipType topicType, Predicate<Node> isFinished) {
			this.topicType = topicType;
			this.isFinished = isFinished;
		}

		private DFTraversal goDeep(Node n, DFTraversal dft) {
			dft.addToPath(n);

			if (isFinished.test(n)) {
				dft.complete();
				return dft;
			}

			Optional<DFTraversal> successfulTraversal = getChildrenSorted(n, topicType)
					.filter(not(dft::containsAndAdd))
					.map(child -> goDeep(child, dft))
					.filter(DFTraversal::isCompleted)
					.findFirst();

			return successfulTraversal
					.orElseGet(() -> {
						dft.popFromPath();
						return dft;
					});
		};
	}

	private static Stream<Relationship> getRelationshipsSorted(Node n, RelationshipType topicType) {
		return goStream(n.getRelationships(Direction.OUTGOING, TRUSTS, topicType))
				.sorted(sortTopicFirstThenRank(topicType));
	}

	private static Stream<Node> getChildrenSorted(Node n, RelationshipType topicType) {
		return getRelationshipsSorted(n, topicType)
				.map(Relationship::getEndNode);
	}

	private static Comparator<Relationship> sortTopicFirstThenRank(RelationshipType topicType) {
		return (left, right) -> {
			if (left.isType(topicType)) {
				return -1;
			}

			if (right.isType(topicType)) {
				return 1;
			}

			// must both be trusts
			return rankComparator.compare(left, right);
		};
	}
}
