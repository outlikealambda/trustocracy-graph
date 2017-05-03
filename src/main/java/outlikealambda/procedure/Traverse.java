package outlikealambda.procedure;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import outlikealambda.output.FriendAuthor;
import outlikealambda.output.Influence;
import outlikealambda.output.TraversalResult;
import outlikealambda.traversal.Nodes;
import outlikealambda.traversal.Relationships;
import outlikealambda.traversal.walk.Navigator;
import outlikealambda.utils.Traversals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
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

	public static class UserRelation {
		// public for serialization
		public final String name;
		public final Long id;
		public final List<String> relationships;
		public final Long rank;
		public final Boolean isRanked;
		public final Boolean isManual;
		public final Boolean isInfluencer;

		private UserRelation(
				String name,
				Long id,
				List<String> relationships,
				Long rank,
				Boolean isRanked,
				Boolean isManual,
				Boolean isInfluencer) {
			this.name = name;
			this.id = id;
			this.relationships = relationships;
			this.rank = rank;
			this.isRanked = isRanked;
			this.isManual = isManual;
			this.isInfluencer = isInfluencer;
		}

		public static UserRelation create(Node self, List<Relationship> relationships, boolean isInfluencer) {
			List<String> relationshipNames = new ArrayList<>();
			long rank = -1;
			boolean isRanked = false;
			boolean isManual = false;

			for (Relationship r: relationships) {
				relationshipNames.add(r.getType().name());

				if (isRanked(r)) {
					isRanked = true;
					rank = Relationships.getRank(r);
				}

				if (isManual(r)) {
					isManual = true;
				}
			}

			return new UserRelation(
					(String) self.getProperty("name"),
					(long) self.getProperty("id"),
					relationshipNames,
					rank,
					isRanked,
					isManual,
					isInfluencer
			);
		}

		private Map<String, Object> toMap() {
			Map<String, Object> asMap = new HashMap<>();
			asMap.put("name", name);
			asMap.put("id", id);
			asMap.put("relationships", relationships);
			asMap.put("rank", rank);
			asMap.put("isRanked", isRanked);
			asMap.put("isManual", isManual);
			asMap.put("isInfluencer", isInfluencer);

			return asMap;
		}
	}

	@Procedure("friend")
	public Stream<UserRelation> friend(
			@Name("userId") long userId
	) {
		Node user = getPerson(userId);

		return Relationships.getRankedOutgoing(user)
				.map(r ->  UserRelation.create(r.getEndNode(), Collections.singletonList(r), false));
	}

	@Procedure("friend.author")
	public Stream<FriendAuthor> friendAuthor(
			@Name("userId") long userId,
			@Name("topicId") long topicId
	) {
		Node user = getPerson(userId);
		Navigator navigator = new Navigator(topicId);

		// find the users target
		Optional<Node> maybeTarget = Optional.of(user)
				.filter(navigator::isConnected)
				.map(navigator::getConnectionOut)
				.map(Relationship::getEndNode);

		// find the users neighbors (ranked and/or manual connections)
		Map<Node, List<Relationship>> directRelations = navigator.getRankedAndManualOut(user)
				.collect(Collectors.groupingBy(Relationship::getEndNode));

		Function <Node, UserRelation> getUserRelation = n -> UserRelation.create(
				n,
				Optional.of(n)
						.map(directRelations::get)
						.orElseGet(ArrayList::new),
				maybeTarget.filter(n::equals).isPresent()
		);

		// follow the neighbors to their targets, serialize the results
		return directRelations.keySet().stream()
				.map(neighbor -> new FriendAuthor(
						getUserRelation.apply(neighbor).toMap(),
						Optional.of(neighbor)
								.filter(navigator::isConnected)
								.map(n -> Traversals.follow(navigator, n))
								.map(getUserRelation)
								.map(UserRelation::toMap)
								.orElse(null)
				));
	}

	private static boolean isRanked(Relationship r) {
		return Relationships.Types.ranked().equals(r.getType());
	}

	private static boolean isManual(Relationship r) {
		return r.getType().name().startsWith("MANUAL");
	}

	private Node getPerson(long userId) {
		return gdb.findNode(Nodes.Labels.PERSON, Nodes.Fields.ID, userId);
	}
}
