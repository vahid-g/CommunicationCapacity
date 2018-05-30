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
import java.util.stream.Collectors;

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

	private static final String TBL_QUESTIONS = "questions_s_test_train";
	private static final String TBL_MULTI_ANSWER = "answers_s_2";

	public static void main(String[] args) throws IOException, SQLException {
		String indexName = args[0];
		// set the next arg to a small fraction like 0.01 to find the effective subset
		// with a few queries
		Double trainQuerySetSize = Double.parseDouble(args[1]);
		StackQueryingExperiment sqe = new StackQueryingExperiment();
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable(TBL_QUESTIONS);
		Collections.shuffle(questions, new Random(100));
		questions = questions.subList(0, (int) (trainQuerySetSize * questions.size()));
		LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
		sqe.loadMultipleAnswersForQuestions(questions, TBL_MULTI_ANSWER);
		List<StackQueryAnswer> results = sqe.submitQueriesInParallelWithMultipleAnswers(questions,
				"/data/ghadakcv/stack_index_s_2/" + indexName);
		LOGGER.log(Level.INFO, "querying done!");
		sqe.printResults(results, "/data/ghadakcv/stack_results_recall/");
		LOGGER.log(Level.INFO, "experiment done!");
	}

	protected void submitQueries(List<QuestionDAO> questions, String indexPath) {
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

	protected void submitQueriesInParallel(List<QuestionDAO> questions, String indexPath) {
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

	protected List<StackQueryAnswer> submitQueriesInParallelWithMultipleAnswers(List<QuestionDAO> questions,
			String indexPath) {
		List<StackQueryAnswer> results = new ArrayList<StackQueryAnswer>();
		LOGGER.log(Level.INFO, "retrieving queries..");
		try (IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			LOGGER.log(Level.INFO, "number of tuples in index: {0}", reader.getDocCount(StackIndexer.BODY_FIELD));
			LOGGER.log(Level.INFO, "querying..");
			results = questions.parallelStream()
					.map(question -> submitQuestionWithMultipleAnswers(searcher, analyzer, question))
					.collect(Collectors.toList());
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return results;
	}

	protected StackQueryAnswer submitQuestion(IndexSearcher searcher, Analyzer analyzer, QuestionDAO question) {
		StackQueryAnswer sqa = new StackQueryAnswer(question);
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
					sqa.rrank = 1.0 / (i + 1);
					break;
				}
			}
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, "Couldn't parse query " + question.id);
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return sqa;
	}

	protected StackQueryAnswer submitQuestionWithMultipleAnswers(IndexSearcher searcher, Analyzer analyzer,
			QuestionDAO question) {
		StackQueryAnswer sqa = new StackQueryAnswer(question);
		try {
			String queryText = question.text.replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("\\s+", " ");
			// in the next line, to lower case is necessary to change AND to and, otherwise
			// lucene would consider it as an operator
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
			parser.setDefaultOperator(Operator.OR);
			Query query = parser.parse(queryText.toLowerCase());
			ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
			double tp = 0;
			double tp20 = 0;
			double tp50 = 0;
			for (int i = 0; i < hits.length; i++) {
				Document doc = searcher.doc(hits[i].doc);
				if (question.allAnswers.contains(Integer.parseInt(doc.get(StackIndexer.ID_FIELD)))) {
					if (sqa.rrank == 0) {
						sqa.rrank = 1.0 / (i + 1);
					}
					if (i <= 20) {
						tp20++;
					}
					if (i <= 50) {
						tp50++;
					}
					tp++;
				}
			}
			if (question.allAnswers.size() > 0) {
				sqa.recall = tp / question.allAnswers.size();
				sqa.rec50 = tp50 / 50;
			} else {
				LOGGER.log(Level.SEVERE, "query doesn't have answer: " + question.id);
				LOGGER.log(Level.SEVERE, "setting recall to 1!");
				sqa.recall = 1;
				sqa.rec50 = 1;
			}
			sqa.p20 = tp20 / 20;
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, "Couldn't parse query " + question.id);
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return sqa;
	}

	public List<QuestionDAO> loadQuestionsFromTable(String questionTable) throws IOException, SQLException {
		String query = "select Id, Title, AcceptedAnswerId, TestViewCount, TrainViewCount from stack_overflow."
				+ questionTable + ";";
		return loadQuestions(query);
	}

	public List<QuestionDAO> loadQuestionsFromTable(String questionTable, int limit) throws IOException, SQLException {
		String query = "select Id, Title, AcceptedAnswerId, TestViewCount, TrainViewCount from stack_overflow."
				+ questionTable + " limit " + limit + ";";
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

	protected void loadMultipleAnswersForQuestions(List<QuestionDAO> questions, String table)
			throws IOException, SQLException {
		LOGGER.log(Level.INFO, "Loading multiple answers from database..");
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW)) {
			Connection conn = dc.getConnection();
			conn.setAutoCommit(false);
			for (QuestionDAO question : questions) {
				try (Statement stmt = conn.createStatement()) {
					stmt.setFetchSize(Integer.MIN_VALUE);
					ResultSet rs = stmt
							.executeQuery("select Id from " + table + " where ParentId = " + question.id + ";");
					while (rs.next()) {
						question.allAnswers.add(rs.getInt("Id"));
					}
				}
			}
		}
	}

	void printResults(List<StackQueryAnswer> results, String output) {
		try (FileWriter fw = new FileWriter(new File(output))) {
			for (StackQueryAnswer result : results) {
				fw.write(result.question.id + "," + result.question.testViewCount + "," + result.question.trainViewCount
						+ "," + result.rrank + "," + result.recall + "," + result.rec50 + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	void printResultsWithVotes(List<StackQueryAnswer> results, String output) {
		try (FileWriter fw = new FileWriter(new File(output))) {
			fw.write("id,score,viewcount,rrank,recall\n");
			for (StackQueryAnswer result : results) {
				QuestionDAO question = result.question;
				fw.write(question.id + "," + question.score + "," + question.viewCount + "," + result.rrank + ","
						+ result.recall + "," + result.p20 + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

}