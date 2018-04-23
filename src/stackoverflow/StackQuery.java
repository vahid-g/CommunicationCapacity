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

public class StackQuery {

	private static final Logger LOGGER = Logger.getLogger(StackQuery.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		StackQuery sqsr = new StackQuery();
		sqsr.runExperiment(args[0]);
	}

	void runExperiment(String experimentNumber) throws IOException, SQLException {
		List<QuestionDAO> questions = loadQueries("questions_aa");
		LOGGER.log(Level.INFO, "number of queries: {0}", questions.size());
		submitQueries(questions, "/data/ghadakcv/stack_index_aa/" + experimentNumber);
		LOGGER.log(Level.INFO, "querying done!");
		try (FileWriter fw = new FileWriter(new File("/data/ghadakcv/stack_results/" + experimentNumber + ".csv"))) {
			for (QuestionDAO question : questions) {
				fw.write(question.id + "," + question.text.replace(',', ' ') + "," + question.viewCount + ","
						+ question.mrr + "\n");
			}
		}
		LOGGER.log(Level.INFO, "experiment done!");
	}

	void submitQueries(List<QuestionDAO> questions, String indexPath) {
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
					// lucene would
					// consider it as an operator
					Query query = parser.parse(queryText.toLowerCase());
					ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
					for (int i = 0; i < hits.length; i++) {
						Document doc = searcher.doc(hits[i].doc);
						if (doc.get(StackIndexer.ID_FIELD).equals(question.answer)) {
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

	List<QuestionDAO> loadQueries(String questionsTable) throws IOException, SQLException {
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		conn.setAutoCommit(false);
		List<QuestionDAO> result = new ArrayList<QuestionDAO>();
		String query = "select Id, Title, AcceptedAnswerId, ViewCount from stack_overflow." + questionsTable + ";";
		try (Statement stmt = conn.createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				String id = rs.getString("Id");
				String title = rs.getString("Title");
				String acceptedAnswerId = rs.getString("AcceptedAnswerId");
				String viewCount = rs.getString("ViewCount");
				QuestionDAO dao = new QuestionDAO(id, title, acceptedAnswerId);
				if (viewCount != null) {
					dao.viewCount = Integer.parseInt(viewCount);
				}
				result.add(dao);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		dc.closeConnection();
		return result;
	}
}