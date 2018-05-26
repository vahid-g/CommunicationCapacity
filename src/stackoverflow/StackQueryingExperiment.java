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
import java.util.Collections;
import java.util.List;
import java.util.Random;
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

public class StackQueryingExperiment {

	private static final Logger LOGGER = Logger.getLogger(StackQueryingExperiment.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		String indexName = args[0];
		// set the next arg to a small fraction like 0.01 to find the effective subset
		// with a few queries
		Double trainQuerySetSize = Double.parseDouble(args[1]);
		StackQueryingExperiment sqe = new StackQueryingExperiment("questions_s_test_train",
				"/data/ghadakcv/stack_index_s_recall/" + indexName, "/data/ghadakcv/stack_results_recall/");
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable();
		Collections.shuffle(questions, new Random(100));
		questions = questions.subList(0, (int) (trainQuerySetSize * questions.size()));
		LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
		sqe.loadMultipleAnswersForQuestions(questions);
		sqe.submitQueriesInParallelWithMultipleAnswers(questions);
		LOGGER.log(Level.INFO, "querying done!");
		try (FileWriter fw = new FileWriter(new File(sqe.outputFolder + indexName + ".csv"))) {
			for (QuestionDAO question : questions) {
				fw.write(question.id + "," + question.testViewCount + "," + question.trainViewCount + ","
						+ question.rrank + "," + question.recall + "\n");
			}
		}
		LOGGER.log(Level.INFO, "experiment done!");
	}

	private String questionTable;
	protected String indexPath;
	String outputFolder;

	public StackQueryingExperiment(String questionTable, String indexPath, String outputFolder) {
		this.questionTable = questionTable;
		this.indexPath = indexPath;
		this.outputFolder = outputFolder;
	}

	protected void submitQueries(List<QuestionDAO> questions) {
		LOGGER.log(Level.INFO, "retrieving queries..");
		try (IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
			parser.setDefaultOperator(Operator.OR);
			LOGGER.log(Level.INFO, "number of tuples in index: {0}", reader.getDocCount(StackIndexer.BODY_FIELD));
			LOGGER.log(Level.INFO, "querying..");
			for (QuestionDAO question : questions) {
				submitQuestion(searcher, analyzer, question);
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
			questions.parallelStream().forEach(question -> submitQuestion(searcher, analyzer, question));
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	protected void submitQueriesInParallelWithMultipleAnswers(List<QuestionDAO> questions) {
		LOGGER.log(Level.INFO, "retrieving queries..");
		try (IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			LOGGER.log(Level.INFO, "number of tuples in index: {0}", reader.getDocCount(StackIndexer.BODY_FIELD));
			LOGGER.log(Level.INFO, "querying..");
			questions.parallelStream()
					.forEach(question -> submitQuestionWithMultipleAnswers(searcher, analyzer, question));
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	protected void submitQuestion(IndexSearcher searcher, Analyzer analyzer, QuestionDAO question) {
		try {
			String queryText = question.text.replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("\\s+", " ");
			// in the next line, to lower case is necessary to change AND to and, otherwise
			// lucene would consider it as an operator
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
			parser.setDefaultOperator(Operator.OR);
			Query query = parser.parse(queryText.toLowerCase());
			ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
			for (int i = 0; i < hits.length; i++) {
				Document doc = searcher.doc(hits[i].doc);
				if (doc.get(StackIndexer.ID_FIELD).equals(question.acceptedAnswer)) {
					question.resultRank = i + 1;
					question.rrank = 1.0 / question.resultRank;
					break;
				}
			}
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, "Couldn't parse query " + question.id);
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	protected void submitQuestionWithMultipleAnswers(IndexSearcher searcher, Analyzer analyzer, QuestionDAO question) {
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
				if (question.allAnswers.contains(Integer.parseInt(doc.get(StackIndexer.ID_FIELD)))) {
					if (question.rrank == 0) {
						question.resultRank = i + 1;
						question.rrank = 1.0 / question.resultRank;
					}
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
	}

	public List<QuestionDAO> loadQuestionsFromTable() throws IOException, SQLException {
		String query = "select Id, Title, AcceptedAnswerId, TestViewCount, TrainViewCount from stack_overflow."
				+ this.questionTable + ";";
		return loadQuestions(query);
	}

	public List<QuestionDAO> loadQuestionsFromTable(int limit) throws IOException, SQLException {
		String query = "select Id, Title, AcceptedAnswerId, TestViewCount, TrainViewCount from stack_overflow."
				+ this.questionTable + " limit " + limit + ";";
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
				String testViewCount = rs.getString("TestViewCount");
				String trainViewCount = rs.getString("TrainViewCount");
				QuestionDAO dao = new QuestionDAO(id, title, acceptedAnswerId);
				dao.testViewCount = Integer.parseInt(testViewCount);
				dao.trainViewCount = Integer.parseInt(trainViewCount);
				result.add(dao);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		dc.close();
		return result;
	}

	protected void loadMultipleAnswersForQuestions(List<QuestionDAO> questions) throws IOException, SQLException {
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

}