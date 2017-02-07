package outlikealambda.traversal;

import org.neo4j.graphdb.Node;
import outlikealambda.traversal.unwind.Unwinder;
import outlikealambda.traversal.walk.Navigator;
import outlikealambda.traversal.walk.CleanBlazer;

public class UnwindAndWalk implements ConnectivityManager {
	private final Navigator nav;
	private final CleanBlazer walker;
	private final Unwinder unwinder;

	public UnwindAndWalk(
			Navigator nav,
			CleanBlazer walker,
			Unwinder unwinder
	) {
		this.nav = nav;
		this.walker = walker;
		this.unwinder = unwinder;
	}

	@Override
	public void setTarget(Node source, Node target) {
		nav.setTarget(source, target);
		unwinder.unwind(source).forEach(walker::go);
	}

	@Override
	public void clearTarget(Node source) {
		nav.setTarget(source, null);
		unwinder.unwind(source).forEach(walker::go);
	}

	@Override
	public void setOpinion(Node author, Node opinion) {
		nav.setOpinion(author, opinion);
		unwinder.unwind(author).forEach(walker::go);
	}

	@Override
	public void clearOpinion(Node author) {
		nav.setOpinion(author, null);
		unwinder.unwind(author).forEach(walker::go);
	}

}
