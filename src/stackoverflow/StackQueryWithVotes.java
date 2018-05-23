package stackoverflow;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StackQueryWithVotes {

	private static final Logger LOGGER = Logger.getLogger(StackQueryWithVotes.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		String indexName = args[0];
		StackQueryingExperiment sqe = new StackQueryingExperiment("questions_s_recall",
				"/data/ghadakcv/stack_index_s_recall/" + indexName, false);
		List<QuestionDAO> questions = sqe.loadQuestionsWithScoresFromTable();
		LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
		sqe.submitQueries(questions);
		LOGGER.log(Level.INFO, "querying done!");
		double counter = 0;
		double sum = 0;
		for (QuestionDAO question : questions) {
			sum += question.testViewCount * question.mrr;
			counter += question.testViewCount;
		}
		LOGGER.log(Level.INFO, "experiment done!");
		LOGGER.log(Level.INFO, "MRR = " + sum / counter);
	}

}