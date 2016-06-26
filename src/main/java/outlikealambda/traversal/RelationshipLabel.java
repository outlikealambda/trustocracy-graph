package outlikealambda.traversal;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;

public enum RelationshipLabel {
	TRUSTS,
	TRUSTS_EXPLICITLY,
	DELEGATES,
	UNKNOWN;

	public static RelationshipLabel fromFriendly(String name) {
		return Arrays.stream(RelationshipLabel.values())
				.filter(label -> label.name().equalsIgnoreCase(name.toUpperCase()))
				.findFirst()
				.orElse(UNKNOWN);
	}

	public static Double getCost(Relationship r) {
		switch (RelationshipLabel.fromFriendly(r.getType().name())) {
			case TRUSTS:
				return 2d;
			case TRUSTS_EXPLICITLY:
				return 1d;
			case DELEGATES:
				return 0d;
			default:
				throw new IllegalArgumentException("Undefined weight for relationship of type: " + r.getType().name());
		}
	}

	public static Predicate<Relationship> isInteresting(long topicId) {
		return relationship ->
				Optional.of(relationship.getType().name())
						.map(RelationshipLabel::fromFriendly)
						.map(label -> {
							switch (label) {
								case TRUSTS:
									return true;
								case TRUSTS_EXPLICITLY:
									return true;
								case DELEGATES:
									return Optional.of(relationship)
											.map(r -> r.getProperty("topicId"))
											.map(tid -> (long) tid)
											.map(tid -> tid == topicId)
											.orElse(false);
								default:
									return false;
							}
						})
						.orElse(false);


	}
}
