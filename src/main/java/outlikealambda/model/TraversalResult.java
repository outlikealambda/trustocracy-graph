package outlikealambda.model;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class TraversalResult {
	// these fields need to be public for Neo4j serialization
	public final Object friend;
	public final Object author;
	public final Object opinion;

	public TraversalResult(Person friend, Optional<Person> author, Optional<Map<String, Object>> opinion) {
		this.friend = friend.toMap();
		this.author = author.map(Person::toMap).orElseGet(Collections::emptyMap);
		this.opinion = opinion.orElseGet(Collections::emptyMap);
	}
}
