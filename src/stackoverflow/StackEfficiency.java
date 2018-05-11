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
		Double trainSize = 0.00001;
		sqsr.runExperiment(trainSize);
	}

	private void runExperiment(double samplePercentage) throws IOException, SQLException {
		List<QuestionDAO> questions = new StackQuery().loadQueriesFromTable("questions_s_test_train");
		if (samplePercentage < 1.0) {
			Collections.shuffle(questions, new Random(100));
			questions = questions.subList(0, (int) (samplePercentage * questions.size()));
		}
		long subsetTime = 0;
		long time = 0;
		try (DatabaseConnection stackConnection = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
				DatabaseConnection abtinConnection = new DatabaseConnection(DatabaseType.ABTIN)) {
			int loop = 10;
			for (int i = 0; i < loop; i++) {
				String subsetQueryTemplate = "SELECT a.Id FROM answers_s_train_18 a left join comments_18 c on a.Id = c.PostId "
						+ "left join posthistory_18 p on a.Id = p.PostId "
						+ "left join postlinks_18 pl on a.Id = pl.PostId "
						+ "left join votes_18 v on a.Id = v.PostId WHERE a.Id in %s;";
				LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
				subsetTime += submitQueries(questions, "/data/ghadakcv/stack_index_s/18", subsetQueryTemplate,
						stackConnection);
				LOGGER.log(Level.INFO, "querying done!");

				String queryTemplate = "SELECT a.Id FROM answers_s_train a left join Comments c on a.Id = c.PostId "
						+ "left join PostHistory p on a.Id = p.PostId " + "left join PostLinks pl on a.Id = pl.PostId "
						+ "left join Votes v on a.Id = v.PostId WHERE a.Id in %s;";
				LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
				time += submitQueries(questions, "/data/ghadakcv/stack_index_s/100", queryTemplate, abtinConnection);
				LOGGER.log(Level.INFO, "querying done!");
			}
			LOGGER.log(Level.INFO,
					"subset time per query = " + (subsetTime / loop / questions.size()) + " milli seconds");
			LOGGER.log(Level.INFO, "db time per query = " + (time / loop / questions.size()) + " milli seconds");
		}
	}

	private long submitQueries(List<QuestionDAO> questions, String indexPath, String queryPrefix,
			DatabaseConnection dc) {
		LOGGER.log(Level.INFO, "retrieving queries..");
		long time = 0;
		try (IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
			parser.setDefaultOperator(Operator.OR);
			LOGGER.log(Level.INFO, "number of tuples in index: {0}", reader.getDocCount(StackIndexer.BODY_FIELD));
			Connection conn = dc.getConnection();
			conn.setAutoCommit(false);
			LOGGER.log(Level.INFO, "querying..");
			long startTime = System.currentTimeMillis();
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