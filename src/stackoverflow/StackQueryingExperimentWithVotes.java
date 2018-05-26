package stackoverflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.NIOFSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;

public class StackQueryingExperimentWithVotes extends StackQueryingExperiment {

	private static final Logger LOGGER = Logger.getLogger(StackQueryingExperimentWithVotes.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		String indexName = args[0];
		StackQueryingExperimentWithVotes sqe = new StackQueryingExperimentWithVotes("questions_s_recall",
				"/data/ghadakcv/stack_index_s_recall/" + indexName);
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable();
		sqe.loadMultipleAnswersForQuestions(questions);
		LOGGER.log(Level.INFO, "number of distinct queries: {0}", questions.size());
		sqe.submitQueriesInParallel(questions);
		LOGGER.log(Level.INFO, "querying done!");
		double counter = 0;
		double sum = 0;
		for (QuestionDAO question : questions) {
			sum += question.score * question.rrank;
			counter += question.score;
		}
		LOGGER.log(Level.INFO, "experiment done!");
		LOGGER.log(Level.INFO, "recall = " + sum / counter);
		String output = "/data/ghadakcv/stack_results_recall/" + indexName + ".csv";
		try (FileWriter fw = new FileWriter(new File(output))) {
			fw.write("id,score,viewcount,rrank,recall\n");
			for (QuestionDAO question : questions) {
				fw.write(question.id + "," + question.score + "," + question.viewCount + "," + question.rrank + ","
						+ question.recall + "\n");
			}
		}
		LOGGER.log(Level.INFO, "experiment done!");
	}

	public StackQueryingExperimentWithVotes(String questionTable, String indexPath) {
		super(questionTable, indexPath);
	}

	protected void submitQueries(List<QuestionDAO> questions) {
		LOGGER.log(Level.INFO, "retrieving queries for recall..");
		try (IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
			parser.setDefaultOperator(Operator.OR);
			LOGGER.log(Level.INFO, "number of tuples in index: {0}", reader.getDocCount(StackIndexer.BODY_FIELD));
			LOGGER.log(Level.INFO, "querying..");
			for (QuestionDAO question : questions) {
				try {
					String queryText = question.text.replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("\\s+", " ");
					// in the next line, to lower case is necessary to change AND to and, otherwise
					// lucene would consider it as an operator
					Query query = parser.parse(queryText.toLowerCase());
					ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
					double tp = 0;
					for (int i = 0; i < hits.length; i++) {
						Document doc = searcher.doc(hits[i].doc);
						if (doc.get(StackIndexer.ID_FIELD).equals(question.acceptedAnswer) && question.rrank == 0) {
							question.resultRank = i + 1;
							question.rrank = 1.0 / question.resultRank;
						}
						if (question.allAnswers.contains(Integer.parseInt(doc.get(StackIndexer.ID_FIELD)))) {
							tp++;
						}
					}
					if (question.allAnswers.size() > 0) {
						question.recall = tp / question.allAnswers.size();
					}
				} catch (ParseException e) {
					LOGGER.log(Level.SEVERE, "Couldn't parse query " + question.id);
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	protected void submitQueriesInParallel(List<QuestionDAO> questions) {
		LOGGER.log(Level.INFO, "retrieving queries..");
		try (IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			LOGGER.log(Level.INFO, "number of tuples in index: {0}", reader.getDocCount(StackIndexer.BODY_FIELD));
			LOGGER.log(Level.INFO, "querying..");
			questions.parallelStream().forEach(question -> {
				try {
					String queryText = question.text.replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("\\s+", " ");
					// in the next line, to lower case is necessary to change AND to and, otherwise
					// lucene would consider it as an operator
					QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
					parser.setDefaultOperator(Operator.OR);
					Query query = parser.parse(queryText.toLowerCase());
					ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
					double tp = 0;
					for (int i = 0; i < hits.length; i++) {
						Document doc = searcher.doc(hits[i].doc);
						if (doc.get(StackIndexer.ID_FIELD).equals(question.acceptedAnswer) && question.rrank == 0) {
							question.resultRank = i + 1;
							question.rrank = 1.0 / question.resultRank;
						}
						if (question.allAnswers.contains(Integer.parseInt(doc.get(StackIndexer.ID_FIELD)))) {
							tp++;
						}
					}
					if (question.allAnswers.size() > 0) {
						question.recall = tp / question.allAnswers.size();
					}
				} catch (ParseException e) {
					LOGGER.log(Level.SEVERE, "Couldn't parse query " + question.id);
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			});
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private void loadMultipleAnswersForQuestions(List<QuestionDAO> questions) throws IOException, SQLException {
		LOGGER.log(Level.INFO, "Loading multiple answers from database..");
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
		LOGGER.log(Level.INFO, "Loading questions from database..");
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
				int viewCount = rs.getInt("ViewCount");
				int score = rs.getInt("Score");
				QuestionDAO question = new QuestionDAO(id, title, acceptedAnswerId);
				question.score = score;
				question.viewCount = viewCount;
				result.add(question);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		dc.close();
		return result;
	}
}