package outlikealambda.traversal;

import org.neo4j.graphalgo.impl.util.DijkstraSelectorFactory;
import org.neo4j.graphalgo.impl.util.PathInterest;

import java.util.Comparator;

/**
 * Stateful; create a new one for each traversal
 */
public class WeightedRelationshipSelectorFactory {

	public static DijkstraSelectorFactory create() {
		return new DijkstraSelectorFactory(new PriorityInterest(), (r, ignored) -> RelationshipLabel.getCost(r));
	}

	private static class PriorityInterest implements PathInterest<Double> {
		@Override
		public Comparator<Double> comparator() {
			return Comparable::compareTo;
		}

		@Override
		public boolean canBeRuledOut(int numberOfVisits, Double pathPriority, Double oldPriority) {
			return pathPriority >= oldPriority;
		}

		@Override
		public boolean stillInteresting(int numberOfVisits) {
			return true;
		}

		@Override
		public boolean stopAfterLowestCost() {
			return false;
		}
	}
}
