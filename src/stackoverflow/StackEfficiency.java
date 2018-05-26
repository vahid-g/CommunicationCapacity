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

public class StackEfficiency {

	private static final Logger LOGGER = Logger.getLogger(StackEfficiency.class.getName());

	public static void main(String[] args) throws IOException, SQLException, ParseException {
		StackEfficiency sqsr = new StackEfficiency();
		Double trainSize = 0.00001;
		sqsr.runExperiment(trainSize);
	}

	private void runExperiment(double samplePercentage) throws IOException, SQLException, ParseException {
		StackQueryingExperiment sq = new StackQueryingExperiment();
		List<QuestionDAO> questions = sq.loadQuestionsFromTable("questions_s_test_train");
		if (samplePercentage < 1.0) {
			Collections.shuffle(questions, new Random(100));
			questions = questions.subList(0, (int) (samplePercentage * questions.size()));
		}
		long subsetIndexQueryTime = 0;
		long subsetQueryTime = 0;
		long indexQueryTime = 0;
		long queryTime = 0;
		try (IndexReader subsetIndexReader = DirectoryReader
				.open(NIOFSDirectory.open(Paths.get("/data/ghadakcv/stack_index_s/18")));
				IndexReader indexReader = DirectoryReader
						.open(NIOFSDirectory.open(Paths.get("/data/ghadakcv/stack_index_s/10")));
				DatabaseConnection subsetConnection = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
				DatabaseConnection dbConnection = new DatabaseConnection(DatabaseType.ABTIN)) {
			LOGGER.log(Level.INFO, "number of tuples in subset index: {0}",
					subsetIndexReader.getDocCount(StackIndexer.BODY_FIELD));
			LOGGER.log(Level.INFO, "number of tuples in index: {0}", indexReader.getDocCount(StackIndexer.BODY_FIELD));
			IndexSearcher subsetSearcher = new IndexSearcher(subsetIndexReader);
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, new StandardAnalyzer());
			parser.setDefaultOperator(Operator.OR);
			IndexSearcher searcher = new IndexSearcher(indexReader);
			Connection subsetDatabaseConnection = subsetConnection.getConnection();
			Connection databaseConnection = dbConnection.getConnection();
			subsetDatabaseConnection.setAutoCommit(false);
			int loop = 10;
			LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
			for (int i = 0; i < loop; i++) {
				LOGGER.log(Level.INFO, "executing loop #" + i);
				long start = System.currentTimeMillis();
				List<List<String>> returnedIds = submitQueriesJustToIndex(questions, subsetSearcher, parser);
				subsetIndexQueryTime += System.currentTimeMillis() - start;
				String subsetQueryTemplate = "SELECT a.Id FROM answers_s_train_18 a left join comments_18 c on a.Id = c.PostId "
						+ "left join posthistory_18 p on a.Id = p.PostId "
						+ "left join postlinks_18 pl on a.Id = pl.PostId "
						+ "left join votes_18 v on a.Id = v.PostId WHERE a.Id in %s;";
				subsetQueryTime += submitQueries(returnedIds, subsetQueryTemplate, subsetDatabaseConnection);
				start = System.currentTimeMillis();
				returnedIds = submitQueriesJustToIndex(questions, searcher, parser);
				indexQueryTime += System.currentTimeMillis() - start;
				String queryTemplate = "SELECT a.Id FROM answers_s_train a left join Comments c on a.Id = c.PostId "
						+ "left join PostHistory p on a.Id = p.PostId " + "left join PostLinks pl on a.Id = pl.PostId "
						+ "left join Votes v on a.Id = v.PostId WHERE a.Id in %s;";
				queryTime += submitQueries(returnedIds, queryTemplate, databaseConnection);
			}
			LOGGER.log(Level.INFO,
					"subset query time for index and db time per query = "
							+ (subsetIndexQueryTime / loop / questions.size()) + " "
							+ (subsetQueryTime / loop / questions.size()) + " milli seconds");
			LOGGER.log(Level.INFO,
					"db query time for index and db time per query = " + (indexQueryTime / loop / questions.size())
							+ " " + (queryTime / loop / questions.size()) + " milli seconds");
		}
	}

	private List<List<String>> submitQueriesJustToIndex(List<QuestionDAO> questions, IndexSearcher searcher,
			QueryParser parser) throws IOException, ParseException {
		List<List<String>> result = new ArrayList<List<String>>();
		for (QuestionDAO question : questions) {
			List<String> ids = new ArrayList<String>();
			String queryText = question.text.replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("\\s+", " ");
			// in the next line, to lower case is necessary to change AND to and, otherwise
			// lucene would consider it as an operator
			Query query = parser.parse(queryText.toLowerCase());
			ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
			for (int i = 0; i < hits.length; i++) {
				Document doc = searcher.doc(hits[i].doc);
				ids.add(doc.get(StackIndexer.ID_FIELD));
			}
			result.add(ids);
		}
		return result;
	}

	private long submitQueries(List<List<String>> returnedIds, String queryPrefix, Connection conn)
			throws SQLException, IOException {
		long time = 0;
		long startTime = System.currentTimeMillis();
		for (List<String> ids : returnedIds) {
			String sql = String.format(queryPrefix, ids.toString().replace('[', '(').replace(']', ')'));
			retreiveTupleTrees(sql, conn);
		}
		time = System.currentTimeMillis() - startTime;
		return time;
	}

	private long retreiveTupleTrees(String sql, Connection conn) throws SQLException, IOException {
		long start = System.currentTimeMillis();
		try (Statement stmt = conn.createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				rs.getString("Id");
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		long time = (System.currentTimeMillis() - start);

		return time;
	}
}