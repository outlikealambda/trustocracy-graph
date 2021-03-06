package outlikealambda.output;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import outlikealambda.traversal.Relationships;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Person {
	private final String name;
	private final Long id;
	private final String relationship;
	private final Long rank;
	private final Boolean isInfluencer;

	public Person(String name, Long id, String relationship) {
		this(name, id, relationship, -1L);
	}

	public Person(String name, Long id, String relationship, Long rank) {
		this(name, id, relationship, rank, false);
	}

	public Person(String name, Long id, String relationship, Long rank, boolean isInfluencer) {
		this.name = name;
		this.id = id;
		this.relationship = relationship;
		this.rank = rank;
		this.isInfluencer = isInfluencer;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> asMap = new HashMap<>();
		asMap.put("name", name);
		asMap.put("id", id);
		asMap.put("relationship", relationship);
		asMap.put("rank", rank);
		asMap.put("isInfluencer", isInfluencer);

		return asMap;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Person))
			return false;
		if (obj == this)
			return true;

		Person rhs = (Person) obj;
		return new EqualsBuilder()
				.append(name, rhs.name)
				.append(id, rhs.id)
				.append(relationship, rhs.relationship)
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(3, 71)
				.append(name)
				.append(id)
				.append(relationship)
				.toHashCode();
	}

	public static Person create(Node n, Relationship r) {
		return create(n, r, false);
	}

	public static Person create(Node n, Relationship r, boolean isInfluencer) {
		String name = (String) n.getProperty("name");

		long id = (long) n.getProperty("id");

		String relationship = Optional.ofNullable(r)
				.map(Relationship::getType)
				.map(RelationshipType::name)
				.orElse("NONE");

		Long rank = Optional.ofNullable(r)
				.map(Relationships::getRank)
				.orElse(-1L);

		return new Person(name, id, relationship, rank, isInfluencer);
	}
}
