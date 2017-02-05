package outlikealambda.traversal.unwind;

import org.neo4j.graphdb.Node;

import java.util.LinkedHashSet;

public interface Unwinder {
	LinkedHashSet<Node> unwind(Node start);
}
