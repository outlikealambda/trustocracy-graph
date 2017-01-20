package outlikealambda.traversal;

import org.neo4j.driver.v1.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TestUtils {
	public static boolean containsFriendAuthorNameCombo(String friendName, String authorName, List<Record> records) {
		return records.stream()
				.filter(isFriend(friendName))
				.anyMatch(r -> r.get("author").asMap().get("name").equals(authorName));
	}

	public static boolean containsFriendWithoutAuthor(String friendName, List<Record> records) {
		return records.stream()
				.filter(isFriend(friendName))
				.anyMatch(r -> r.get("author").asMap().get("name") == null);
	}

	public static boolean friendIsInfluencer(String friendName, List<Record> records) {
		return records.stream()
				.filter(isFriend(friendName))
				.anyMatch(record -> record.get("friend").asMap().get("isInfluencer").equals(true));
	}

	private static Predicate<Record> isFriend(String friendName) {
		return r -> r.get("friend").asMap().get("name").equals(friendName);
	}

	public static class Builder {
		private List<String> creates = new ArrayList<>();

		public String build() {
			return "CREATE" + creates.stream().collect(Collectors.joining(","));
		}

		public Builder addPerson(String person, int id) {
			creates.add(String.format("(%s:Person {name:'%s', id:%d})", person, person, id));
			return this;
		}

		public Builder addOpinion(String opinion, int id) {
			creates.add(String.format("(%s:Opinion {id:%d})", opinion, id));
			return this;
		}

		public Builder connectManual(String source, String target, Relationships.Topic topic) {
			creates.add(connect(source, target, topic.getManualType().name()));
			return this;
		}

		public Builder connectProvisional(String source, String target, Relationships.Topic topic) {
			creates.add(connect(source, target, topic.getProvisionalType().name()));
			return this;
		}

		public Builder connectAuthored(String source, String target, Relationships.Topic topic) {
			creates.add(connect(source, target, topic.getAuthoredType().name()));
			return this;
		}

		public Builder connectRanked(String source, String target, int rank) {
			creates.add(connect(source, target, String.format("RANKED {rank:%d}", rank)));
			return this;
		}

		private static String connect(String source, String target, String r) {
			return String.format("(%s)-[:%s]->(%s)", source, r, target);
		}
	}
}
