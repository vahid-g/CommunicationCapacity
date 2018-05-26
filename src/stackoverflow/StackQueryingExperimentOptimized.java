package stackoverflow;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StackQueryingExperimentOptimized {

	private static final Logger LOGGER = Logger.getLogger(StackQueryingExperimentOptimized.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		StackQueryingExperiment sqe = new StackQueryingExperiment("questions_s_test_train",
				"/data/ghadakcv/stack_index_s_recall/index", "/data/ghadakcv/stack_results_recall/");
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable();
		Collections.shuffle(questions, new Random(100));
		LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
		sqe.loadMultipleAnswersForQuestions(questions);

		for (int i = 1; i <= 100; i++) {
			List<StackQueryAnswer> results = sqe.submitQueriesInParallelWithMultipleAnswers(questions);
			LOGGER.log(Level.INFO, "querying done!");
			sqe.printResults(results, "/data/ghadakcv/stack_results_recall/" + i + ".csv");
			LOGGER.log(Level.INFO, "experiment " + i + " done!");
		}

	}
}