package outlikealambda.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Journey {
	private final Person trustee;
	private final List<String> hops;

	public Journey(Person trustee, List<String> hops) {
		this.trustee = trustee;
		this.hops = hops;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> asMap = new HashMap<>();
		asMap.put("trustee", trustee.toMap());
		asMap.put("hops", hops);

		return asMap;
	}
}
