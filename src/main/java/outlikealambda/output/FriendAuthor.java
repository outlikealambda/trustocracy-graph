package outlikealambda.output;

import java.util.Map;

public class FriendAuthor {

	public final Object friend;
	public final Object author;

	public FriendAuthor(Map<String, Object> friend, Map<String, Object> author) {
		this.friend = friend;
		this.author = author;
	}
}
