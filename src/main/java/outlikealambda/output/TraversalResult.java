package outlikealambda.output;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import outlikealambda.utils.Traversals;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class TraversalResult {
	// these fields need to be public for Neo4j serialization
	public final Object friend;
	public final Object author;
	public final Object opinion;

	public TraversalResult(Person friend, Person author, Map<String, Object> opinion) {
		this.friend = friend.toMap();

		this.author = Optional.ofNullable(author)
				.map(Person::toMap)
				.orElseGet(Collections::emptyMap);

		this.opinion = Optional.ofNullable(opinion).orElseGet(Collections::emptyMap);
	}

	public static Stream<TraversalResult> mergeIntoTraversalResults(
			Map<Node, Relationship> friendLinks,
			Map<Node, Node> friendAuthors,
			Map<Node, Node> authorOpinions,
			Optional<Node> target
	) {
		return friendLinks.entrySet().stream()
				.sorted((friendAndRel1, friendAndRel2) ->
						Traversals.rankComparator.compare(
								friendAndRel1.getValue(),
								friendAndRel2.getValue()))
				.map(friendAndRel -> {
					Node friend = friendAndRel.getKey();
					Relationship friendRel = friendAndRel.getValue();
					Person friendPerson = Person.create(friend, friendRel, target.map(friend::equals).orElse(false));

					return Optional.ofNullable(friendAuthors.get(friend))
							.map(author -> new TraversalResult(
									friendPerson,
									Person.create(author, friendLinks.get(author)),
									authorOpinions.get(author).getAllProperties())
							)
							.orElseGet(() -> new TraversalResult(
									friendPerson,
									null,
									null)
							);
				});
	}
}
