package outlikealambda.model;

import java.util.List;
import java.util.Map;

public class Connection {
	public final List<Map<String, Object>> paths;
	public final Map<String, Object> opinion;

	public Connection(List<Map<String, Object>> paths, Map<String, Object> opinion, Map<String, Object> author, Map<String, Object> qualifications) {
		this.paths = paths;
		this.opinion = opinion;
		this.opinion.put("author", author);
		this.opinion.put("qualifications", qualifications);
	}
}
