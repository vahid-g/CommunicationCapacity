package stackoverflow;

import java.util.ArrayList;
import java.util.List;

public class QuestionDAO {

	public String id;
	public String text;
	public String acceptedAnswer;
	public List<Integer> allAnswers = new ArrayList<Integer>();
	public int resultRank = -1;
	public int viewCount = 0;
	public int testViewCount = 0;
	public int trainViewCount = 0;
	public int score = 0;
	public double rrank = 0;
	public double recall = 0;

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
