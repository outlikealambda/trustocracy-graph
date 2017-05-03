package outlikealambda.output;

import java.util.Map;

/**
 * Using this class allows us to name properties.  If we just serialize
 * Traverse.UserRelation directly, the class fields become items in the
 * results.row array.
 *
 * Wrapping a Map lets us define the property names, and forces them
 * into a json object in the results.
 */
public class Friend {
	public final Map<String, Object> friend;

	public Friend(Map<String, Object> friend) {
		this.friend = friend;
	}
}
