package outlikealambda.traversal;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.PathEvaluator;

public final class DistanceEvaluators {

	public static PathEvaluator<Double> withinDistance(Double maxDistance) {
		return new PathEvaluator.Adapter<Double>() {
			@Override public Evaluation evaluate(Path path, BranchState<Double> state) {
				if (state.getState() < maxDistance) return Evaluation.INCLUDE_AND_CONTINUE;

				return Evaluation.EXCLUDE_AND_PRUNE;
			}
		};
	}
}
