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

public class StackExperiment {

	private static final Logger LOGGER = Logger.getLogger(StackExperiment.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		StackExperiment se = new StackExperiment();
		List<QuestionDAO> questions = se.loadQueries();
		try (IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get("/data/ghadakcv/stack_index")))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
			parser.setDefaultOperator(Operator.OR);
			LOGGER.log(Level.INFO, "querying..");
			for (QuestionDAO question : questions) {
				try {
					String queryText = question.text.replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("\\s+", " ");
					// String queryText = question.text.replace("()", " ");
					// queryText = queryText.replace("&#xA;", " ");
					// queryText = queryText.replaceAll("<[^>]*>", " ");
					// queryText = queryText.replaceAll("[%\"\\[\\]+-=.:*?/;'{}()\\\\]", " ");

					// System.out.println("qid: " + question.id + " rel: " + question.answer);
					// System.out.println(queryText);
					Query query = parser.parse(queryText);
					ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
					// int counter = 0;
					for (int i = 0; i < hits.length; i++) {
						Document doc = searcher.doc(hits[i].doc);
						if (doc.get(StackIndexer.ID_FIELD).equals(question.answer)) {
							question.resultRank = i + 1;
							break;
						}
						// if (counter++ < 3) {
						// System.out.println("\t" + doc.get(StackIndexer.ID_FIELD));
						// }
					}
					// System.out.println();
				} catch (ParseException e) {
					LOGGER.log(Level.SEVERE, "Couldn't parse query " + question.id);
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
			try (FileWriter fw = new FileWriter(new File("stack_2.csv"))) {
				for (QuestionDAO question : questions) {
					if (question.resultRank != -1) {
						fw.write(question.id + "," + question.resultRank);
					}
				}
			}
		}
	}

	private List<QuestionDAO> loadQueries() throws IOException, SQLException {
		// setting up database connections
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		conn.setAutoCommit(false);
		List<QuestionDAO> result = new ArrayList<QuestionDAO>();
		LOGGER.log(Level.INFO, "retrieving queries..");
		String query = "select Id, Title, AcceptedAnswerId from stack_overflow.questions"
				+ " where AcceptedAnswerId is not null and length(Body) < 5000 limit 20000;";
		try (Statement stmt = conn.createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				String id = rs.getString("Id");
				String title = rs.getString("Title");
				// String body = rs.getString("Body");
				String acceptedAnswerId = rs.getString("AcceptedAnswerId");
				QuestionDAO dao = new QuestionDAO(id, title, acceptedAnswerId);
				result.add(dao);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		dc.closeConnection();
		return result;
	}
}