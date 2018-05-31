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
		String outputFolder = "/data/ghadakcv/stack_results_recall/";
		if (args.length > 0) {
			outputFolder = args[0];
		}
		StackQueryingExperiment sqe = new StackQueryingExperiment();
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable("questions_s_test_train");
		sqe.loadMultipleAnswersForQuestions(questions, StackQueryingExperiment.TBL_MULTI_ANSWER);
		Collections.shuffle(questions, new Random(100));
		LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
		for (int i = 1; i <= 100; i += 1) {
			LOGGER.log(Level.INFO, "running loop: " + i);
			List<StackQueryAnswer> results = sqe.submitQueriesInParallelWithMultipleAnswers(questions,
					"/data/ghadakcv/stack_index_s_recall/" + i);
			LOGGER.log(Level.INFO, "querying done!");
			sqe.printResults(results, outputFolder + i + ".csv");
			LOGGER.log(Level.INFO, "experiment " + i + " done!");
		}
	}
}