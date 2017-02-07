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


	public static Walkable createWalkable(int topicId) {
		return new Walkable(topicId);
	}

	public static class Walkable {
		private List<String> creates = new ArrayList<>();
		private final int topicId;

		public Walkable(int topicId) {
			this.topicId = topicId;
		}

		public String build() {
			return "CREATE" + creates.stream().collect(Collectors.joining(","));
		}

		public Walkable addPersonIdOnly(int id) {
			creates.add(String.format("(p"+id+":Person {id:%d})", id));
			return this;
		}

		public Walkable addOpinionIdOnly(int id) {
			creates.add(String.format("(o"+id+":Opinion {id:%d})", id));
			return this;
		}

		public Walkable addPerson(String person, int id) {
			creates.add(String.format("(%s:Person {name:'%s', id:%d})", person, person, id));
			return this;
		}

		public Walkable addDisjointPerson(String person, int id) {
			creates.add(String.format("(%s:Person {name:'%s', id:%d, disjoint:'true'})", person, person, id));
			return this;
		}

		public Walkable addOpinion(String opinion, int id) {
			creates.add(String.format("(%s:Opinion {id:%d})", opinion, id));
			return this;
		}

		public Walkable connectManual(String source, String target) {
			creates.add(connect(source, target, RelationshipTypes.manual(topicId).name()));
			return this;
		}

		public Walkable connectAuthored(String source, String target) {
			creates.add(connect(source, target, RelationshipTypes.authored(topicId).name()));
			return this;
		}

		public Walkable connectRanked(String source, String target, int rank) {
			creates.add(connect(source, target, String.format("RANKED {rank:%d}", rank)));
			return this;
		}

		public Walkable connectConnected(String source, String target) {
			creates.add(connect(source, target, RelationshipTypes.connected(topicId).name()));
			return this;
		}

		public Walkable connectRankedById(int source, int target, int rank) {
			creates.add(connect("p"+source, "p"+target, String.format("RANKED {rank:%d}", rank)));
			return this;
		}

		private static String connect(String source, String target, String r) {
			return String.format("(%s)-[:%s]->(%s)", source, r, target);
		}
	}
}
