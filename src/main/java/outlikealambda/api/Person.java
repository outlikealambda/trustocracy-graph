package outlikealambda.api;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

public class Person {
	private final String name;
	private final Long id;
	private final String relationship;

	public Person(String name, Long id, String relationship) {
		this.name = name;
		this.id = id;
		this.relationship = relationship;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> asMap = new HashMap<>();
		asMap.put("name", name);
		asMap.put("id", id);
		asMap.put("relationship", relationship);

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
}
