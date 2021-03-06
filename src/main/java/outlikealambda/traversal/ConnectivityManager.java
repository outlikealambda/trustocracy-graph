package outlikealambda.traversal;

import org.neo4j.graphdb.Node;
import outlikealambda.traversal.unwind.BasicUnwinder;
import outlikealambda.traversal.unwind.Unwinder;
import outlikealambda.traversal.walk.CleanBlazer;
import outlikealambda.traversal.walk.DirtyBlazer;
import outlikealambda.traversal.walk.Navigator;

import java.util.List;
import java.util.function.Consumer;

public interface ConnectivityManager {
	void updateConnectivity(Node source);

	void setTarget(Node source, Node target);

	void clearTarget(Node source);

	void setOpinion(Node author, Node opinion);

	void clearOpinion(Node author);

	// TODO: Only write relationships with changed ranks
	static void setRanked(Node source, List<Node> ranked) {
		Relationships.setRanked(source, ranked);
	}

	static ConnectivityManager create(Navigator nav, Consumer<Node> update) {
		return new ConnectivityManager() {
			@Override
			public void updateConnectivity(Node source) {
				update.accept(source);
			}

			@Override
			public void setTarget(Node source, Node target) {
				nav.setTarget(source, target);
				update.accept(source);
			}

			@Override public void clearTarget(Node source) {
				nav.setTarget(source, null);
				update.accept(source);
			}

			@Override public void setOpinion(Node author, Node opinion) {
				nav.setOpinion(author, opinion);
				update.accept(author);
			}

			@Override public void clearOpinion(Node author) {
				nav.setOpinion(author, null);
				update.accept(author);
			}
		};
	}

	/**
	 * Initial attempt; non-deterministic
	 */
	static ConnectivityManager unwindAndWalk(long topicId) {
		Navigator nav = new Navigator(topicId);
		Unwinder unwinder = new BasicUnwinder(nav);
		CleanBlazer blazer = new CleanBlazer(nav);

		return ConnectivityManager.create(
				nav,
				node -> unwinder.unwind(node).forEach(blazer::go)
		);
	}

	static ConnectivityManager dirtyWalker(long topicId) {
		Navigator nav = new Navigator(topicId);
		DirtyBlazer blazer = new DirtyBlazer(nav);

		return ConnectivityManager.create(
				nav,
				blazer::go
		);
	}
}
