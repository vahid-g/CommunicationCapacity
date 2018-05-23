package stackoverflow;

public class QuestionDAO {

	public String id;
	public String text;
	public String acceptedAnswer;
	public String allAnswers[];
	public int resultRank = -1;
	public int testViewCount = 0;
	public int trainViewCount = 0;
	public int score = 0;
	public double mrr = 0;

	public QuestionDAO(String id, String question, String answer) {
		this.id = id;
		this.text = question;
		this.acceptedAnswer = answer;
	}
	
	@Override
	public String toString() {
		return id + ", " + text;
	}
}
