package outlikealambda.model;

public class Influence {
	public final long userId;
	public final long topicId;
	public final long influence;

	public Influence(long userId, long topicId, long influence) {
		this.userId = userId;
		this.topicId = topicId;
		this.influence = influence;
	}
}
