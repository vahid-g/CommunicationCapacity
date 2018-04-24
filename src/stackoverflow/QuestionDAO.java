package stackoverflow;

public class QuestionDAO {

	public String id;
	public String text;
	public String answer;
	public int resultRank = -1;
	public int viewCount = 0;
	public double mrr = 0;

	public QuestionDAO(String id, String question, String answer) {
		this.id = id;
		this.text = question;
		this.answer = answer;
	}
	
	@Override
	public String toString() {
		return id + ", " + text;
	}
}
