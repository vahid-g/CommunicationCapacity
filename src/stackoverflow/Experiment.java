package stackoverflow;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

public class Experiment {

	static Logger LOGGER = Logger.getLogger(Experiment.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		DatabaseMediator dm = new DatabaseMediator();
		// List<QuestionDAO> questions = dm.loadQuestions();
		List<AnswerDAO> answers = dm.loadAnswers();
		// TODO: index answers
	}

	protected void loadQueries() {

	}

}
