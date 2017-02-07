package outlikealambda.traversal.walk;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import outlikealambda.utils.Composables;
import outlikealambda.utils.Traversals;

import java.util.Optional;

/**
 * Assumes that all nodes in the graph which _may_ change
 * have had their state cleared.
 *
 * i.e. the nodes upstream of the start node are neither connected nor disjoint
 */
public class CleanBlazer implements Blazer.Controller {
	private final Navigator navigator;

	public CleanBlazer(Navigator navigator) {
		this.navigator = navigator;
	}

	@Override
	public void go(Node start) {
		new Blazer(navigator, this).start(start);
	}


	@Override
	public Optional<Blazer.Result> handleProcessed(Node source) {
		return Optional.of(source)
				.filter(Composables.or(navigator::isConnected, navigator::isDisjoint))
				.map(navigator::isConnected)
				.map(Blazer.Result::new);
	}

	@Override
	public Blazer.NodeWalker decorate(Blazer.NodeWalker decorated) {
		return decorated;
	}
}
