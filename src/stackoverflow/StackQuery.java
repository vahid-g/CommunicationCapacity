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
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
		List<QuestionDAO> questions = loadQueries();
		runQueries(questions, experimentNumber);
		try (FileWriter fw = new FileWriter(new File("/data/ghadakcv/stack_results/" + experimentNumber + ".csv"))) {
			for (QuestionDAO question : questions) {
				fw.write(question.id + "," + question.text.replace(',', ' ') + "," + question.viewCount + ","
						+ (1.0 / question.resultRank) + "\n");
			}
		}
	}

	void runParallelExperiment() throws IOException, SQLException {
		StackQuery sqsr = new StackQuery();
		List<QuestionDAO> questions = loadQueries();
		Map<Integer, List<Double>> allResults = IntStream.range(1, 100).parallel().boxed()
				.collect(Collectors.toMap(Function.identity(), i -> sqsr.runQueries(questions, i + "")));
		LOGGER.log(Level.INFO, "writing results to file..");
		try (FileWriter fw = new FileWriter(new File("results.csv"))) {
			for (int j = 0; j < questions.size(); j++) {
				QuestionDAO question = questions.get(j);
				fw.write(question.id + "," + question.text + ",");
				for (int i = 1; i < 100; i++) {
					fw.write(allResults.get(i).get(j) + ",");
				}
				fw.write(allResults.get(100).get(j) + "\n");
			}
		}
	}

	List<Double> runQueries(List<QuestionDAO> questions, String experimentNumber) {
		List<Double> results = new ArrayList<Double>();
		LOGGER.log(Level.INFO, "retrieving queries..");
		try (IndexReader reader = DirectoryReader
				.open(NIOFSDirectory.open(Paths.get("/data/ghadakcv/stack_index/" + experimentNumber)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(StackIndexer.BODY_FIELD, analyzer);
			parser.setDefaultOperator(Operator.OR);
			LOGGER.log(Level.INFO, "querying..");
			for (QuestionDAO question : questions) {
				double mrr = 0;
				try {
					String queryText = question.text.replaceAll("[^a-zA-Z0-9 ]", " ").replaceAll("\\s+", " ");
					Query query = parser.parse(queryText);
					ScoreDoc[] hits = searcher.search(query, 200).scoreDocs;
					for (int i = 0; i < hits.length; i++) {
						Document doc = searcher.doc(hits[i].doc);
						if (doc.get(StackIndexer.ID_FIELD).equals(question.answer)) {
							question.resultRank = i + 1;
							mrr = 1.0 / question.resultRank;
							break;
						}
					}
				} catch (ParseException e) {
					LOGGER.log(Level.SEVERE, "Couldn't parse query " + question.id);
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
				results.add(mrr);
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return results;
	}

	List<QuestionDAO> loadQueries() throws IOException, SQLException {
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		conn.setAutoCommit(false);
		List<QuestionDAO> result = new ArrayList<QuestionDAO>();
		String query = "select qid, Title, AcceptedAnswerId, ViewCount from stack_overflow.questions_s;";
		try (Statement stmt = conn.createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				String id = rs.getString("qid");
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