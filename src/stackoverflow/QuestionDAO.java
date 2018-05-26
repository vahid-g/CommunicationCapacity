package stackoverflow;

import java.util.ArrayList;
import java.util.List;

public class QuestionDAO {

	public final String id;
	public final String text;
	public final String acceptedAnswer;
	public List<Integer> allAnswers = new ArrayList<Integer>();

	int resultRank = 0;

	public int viewCount = 0;
	public int testViewCount = 0;
	public int trainViewCount = 0;
	public int score = 0;

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
