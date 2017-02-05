package outlikealambda.traversal;

import org.neo4j.graphdb.Node;
import outlikealambda.traversal.unwind.Unwinder;
import outlikealambda.traversal.walk.Navigator;
import outlikealambda.traversal.walk.Walker;

public class ConnectivityManager {
	private final Navigator nav;
	private final Walker walker;
	private final Unwinder unwinder;

	public ConnectivityManager(
			Navigator nav,
			Walker walker,
			Unwinder unwinder
	) {
		this.nav = nav;
		this.walker = walker;
		this.unwinder = unwinder;
	}

	public void setTarget(Node source, Node target) {
		nav.setTarget(source, target);
		unwinder.unwind(source)
				.forEach(walker::updateConnectivity);
	}

	public void clearTarget(Node source) {
		nav.setTarget(source, null);
		unwinder.unwind(source)
				.forEach(walker::updateConnectivity);
	}

	public void setOpinion(Node author, Node opinion) {
		nav.setOpinion(author, opinion);
		unwinder.unwind(author)
				.forEach(walker::updateConnectivity);
	}

	public void clearOpinion(Node author) {
		nav.setOpinion(author, null);
		unwinder.unwind(author)
				.forEach(walker::updateConnectivity);
	}

}
