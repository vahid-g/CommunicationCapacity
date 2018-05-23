package stackoverflow;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import database.DatabaseConnection;
import database.DatabaseType;

public class StackQueryingExperimentWithVotes extends StackQueryingExperiment {

	private static final Logger LOGGER = Logger.getLogger(StackQueryingExperimentWithVotes.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		String indexName = args[0];
		StackQueryingExperimentWithVotes sqe = new StackQueryingExperimentWithVotes("questions_s_recall",
				"/data/ghadakcv/stack_index_s_recall/" + indexName, false);
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable();
		sqe.loadMultipleAnswersForQuestions(questions);
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

	public StackQueryingExperimentWithVotes(String questionTable, String indexPath, boolean isParallel) {
		super(questionTable, indexPath, isParallel);
	}

	private void loadMultipleAnswersForQuestions(List<QuestionDAO> questions) throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW)) {
			Connection conn = dc.getConnection();
			conn.setAutoCommit(false);
			for (QuestionDAO question : questions) {
				try (Statement stmt = conn.createStatement()) {
					stmt.setFetchSize(Integer.MIN_VALUE);
					ResultSet rs = stmt
							.executeQuery("select Id from answers_s_recall where ParentId = " + question.id + ";");
					while (rs.next()) {
						question.allAnswers.add(rs.getInt("Id"));
					}
				}
			}
		}
	}

	public List<QuestionDAO> loadQuestionsFromTable() throws IOException, SQLException {
		String query = "select Id, Title, AcceptedAnswerId, ViewCount, Score from stack_overflow.questions_s_recall;";
		return loadQuestions(query);
	}

	protected List<QuestionDAO> loadQuestions(String query) throws IOException, SQLException {
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		conn.setAutoCommit(false);
		List<QuestionDAO> result = new ArrayList<QuestionDAO>();
		try (Statement stmt = conn.createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				String id = rs.getString("Id");
				String title = rs.getString("Title").replace(',', ' ');
				String acceptedAnswerId = rs.getString("AcceptedAnswerId");
				String score = rs.getString("Score");
				QuestionDAO dao = new QuestionDAO(id, title, acceptedAnswerId);
				dao.score = Integer.parseInt(score);
				result.add(dao);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		dc.close();
		return result;
	}
}