package outlikealambda.utils;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import outlikealambda.traversal.walk.Navigator;


public final class Traversals {

	/**
	 * finds the author for a given connected node.
	 * <p>
	 * throws an IllegalArgumentException if node is not connected
	 */
	public static Node follow(Navigator navigator, Node source) {
		if (navigator.isAuthor(source)) {
			return source;
		}

		return Composables.asFunction(navigator::getConnectionOut)
				.andThen(Relationship::getEndNode)
				.andThen(n -> follow(navigator, n))
				.apply(source);
	}

	private Traversals() {}
}

