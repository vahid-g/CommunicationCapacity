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

public class StackEfficiency {

	private static final Logger LOGGER = Logger.getLogger(StackEfficiency.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		StackEfficiency sqsr = new StackEfficiency();
		String experiment = args[0];
		Double trainSize = Double.parseDouble(args[1]); // 0.001
		sqsr.runExperiment(experiment, trainSize);
	}

	private void runExperiment(String experimentNumber, double samplePercentage) throws IOException, SQLException {
		List<QuestionDAO> questions = new StackQuery().loadQueries("questions_s_test_train");
		if (samplePercentage < 1.0) {
			Collections.shuffle(questions, new Random(100));
			questions = questions.subList(0, (int) (samplePercentage * questions.size()));
		}
		String queryTemplate = "SELECT Id FROM answers_s_train_18 a, comments_18 c, posthistory_18 p, postlinks_18 pl, votes_18 v"
				+ "WHERE a.Id = c.PostId, a.Id = p.PostId, a.Id = pl.PostId, a.Id = v.PostId and a.Id in %s;";
		LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
		long time = submitQueries(questions, "/data/ghadakcv/stack_index_s/" + experimentNumber, queryTemplate);
		LOGGER.log(Level.INFO, "querying done!");
		LOGGER.log(Level.INFO, "time per query = " + time / 1000 / questions.size() + " seconds");
	}

	private long submitQueries(List<QuestionDAO> questions, String indexPath, String queryPrefix) {
		LOGGER.log(Level.INFO, "retrieving queries..");
		long time = 0;
		try (IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
			parser.setDefaultOperator(Operator.OR);
			LOGGER.log(Level.INFO, "number of tuples in index: {0}", reader.getDocCount(StackIndexer.BODY_FIELD));
			DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
			Connection conn = dc.getConnection();
			conn.setAutoCommit(false);
			LOGGER.log(Level.INFO, "querying..");
			long startTime = 0;
			for (QuestionDAO question : questions) {
				try {
					String queryText = question.text.replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("\\s+", " ");
					// in the next line, to lower case is necessary to change AND to and, otherwise
					// lucene would consider it as an operator
					Query query = parser.parse(queryText.toLowerCase());
					ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
					List<String> ids = new ArrayList<String>();
					for (int i = 0; i < hits.length; i++) {
						Document doc = searcher.doc(hits[i].doc);
						ids.add(doc.get(StackIndexer.ID_FIELD));
					}
					String sql = String.format(queryPrefix, ids.toString().replace('[', '(').replace(']', ')'));
					retreiveTupleTrees(sql, conn);
				} catch (ParseException e) {
					LOGGER.log(Level.SEVERE, "Couldn't parse query " + question.id);
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
			time = System.currentTimeMillis() - startTime;
			dc.closeConnection();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
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