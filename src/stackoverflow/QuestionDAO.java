package stackoverflow;

public class QuestionDAO {

	String id;

	String text;

	String answer;
	
	int resultRank = -1;

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
