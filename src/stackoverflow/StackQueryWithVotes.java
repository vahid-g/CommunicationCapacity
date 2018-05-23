package stackoverflow;

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

public class StackQueryWithVotes {

	private static final Logger LOGGER = Logger.getLogger(StackQueryWithVotes.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		StackQueryWithVotes sqsr = new StackQueryWithVotes();
		String experiment = args[0];
		Double trainSize = Double.parseDouble(args[1]);
		if (args.length > 2 && args[2].equals("-parallel")) {
			sqsr.runExperiment(experiment, true, trainSize);
		} else {
			sqsr.runExperiment(experiment, false, trainSize);
		}
	}

	private void runExperiment(String experimentNumber, boolean parallel, double samplePercentage)
			throws IOException, SQLException {
		List<QuestionDAO> questions = loadQueriesFromTable("questions_s_recall");
		// String outputFile = "/data/ghadakcv/stack_results_recall/" + experimentNumber
		// + ".csv";
		if (samplePercentage < 1.0) {
			Collections.shuffle(questions, new Random(100));
			questions = questions.subList(0, (int) (samplePercentage * questions.size()));
			// outputFile = "/data/ghadakcv/stack_results_recall/train_" + experimentNumber
			// + ".csv";
		}
		LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
		if (parallel) {
			submitParallelQueries(questions, "/data/ghadakcv/stack_index_s_recall/" + experimentNumber);
		} else {
			submitQueries(questions, "/data/ghadakcv/stack_index_s_recall/" + experimentNumber);
		}
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

	private void submitQueries(List<QuestionDAO> questions, String indexPath) {
		LOGGER.log(Level.INFO, "retrieving queries..");
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
					for (int i = 0; i < hits.length; i++) {
						Document doc = searcher.doc(hits[i].doc);
						if (doc.get(StackIndexer.ID_FIELD).equals(question.acceptedAnswer)) {
							question.resultRank = i + 1;
							question.mrr = 1.0 / question.resultRank;
							break;
						}
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

	private void submitParallelQueries(List<QuestionDAO> questions, String indexPath) {
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
					for (int i = 0; i < hits.length; i++) {
						Document doc = searcher.doc(hits[i].doc);
						if (doc.get(StackIndexer.ID_FIELD).equals(question.acceptedAnswer)) {
							question.resultRank = i + 1;
							question.mrr = 1.0 / question.resultRank;
							break;
						}
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

	public List<QuestionDAO> loadQueriesFromTable(String questionsTable) throws IOException, SQLException {
		String query = "select Id, Title, AcceptedAnswerId, ViewCount, Score from stack_overflow." + questionsTable
				+ ";";
		return loadQueries(query);
	}

	public List<QuestionDAO> loadQueriesFromTable(String questionsTable, int limit) throws IOException, SQLException {
		String query = "select Id, Title, AcceptedAnswerId, ViewCount, Score from stack_overflow." + questionsTable
				+ " limit " + limit + ";";
		return loadQueries(query);
	}

	private List<QuestionDAO> loadQueries(String query) throws IOException, SQLException {
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
				String viewCount = rs.getString("ViewCount");
				String score = rs.getString("Score");
				QuestionDAO dao = new QuestionDAO(id, title, acceptedAnswerId);
				dao.testViewCount = Integer.parseInt(score);
				dao.trainViewCount = Integer.parseInt(viewCount);
				result.add(dao);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		dc.close();
		return result;
	}
}