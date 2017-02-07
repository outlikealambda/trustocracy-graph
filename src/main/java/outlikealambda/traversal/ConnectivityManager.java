package outlikealambda.traversal;

import org.neo4j.graphdb.Node;

public interface ConnectivityManager {
	void setTarget(Node source, Node target);

	void clearTarget(Node source);

	void setOpinion(Node author, Node opinion);

	void clearOpinion(Node author);
}
