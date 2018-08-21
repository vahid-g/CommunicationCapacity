package stackoverflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import query.ExperimentQuery;
import query.Qrel;

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

	public String getText() {
		return text;
	}

	@Override
	public String toString() {
		return id + ", " + text;
	}

	public static List<ExperimentQuery> convertToExperimentQuery(List<QuestionDAO> questionList) {
		List<ExperimentQuery> queryList = new ArrayList<ExperimentQuery>();
		for (QuestionDAO question : questionList) {
			Set<Qrel> rels = new HashSet<Qrel>();
			if (!question.allAnswers.isEmpty()) {
				for (Integer id : question.allAnswers) {
					rels.add(new Qrel(Integer.parseInt(question.id), id.toString(), 1));
				}
			} else {
				rels.add(new Qrel(Integer.parseInt(question.id), question.acceptedAnswer + "", 1));
			}
			ExperimentQuery query = new ExperimentQuery(Integer.parseInt(question.id), question.text,
					question.viewCount, rels);
			queryList.add(query);
		}
		return queryList;
	}
}
