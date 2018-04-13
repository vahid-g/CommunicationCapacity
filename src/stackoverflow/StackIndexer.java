package stackoverflow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;

public class StackIndexer {

	static final String ID_FIELD = "id";

	static final String BODY_FIELD = "Body";

	static final String VIEW_COUNT_FIELD = "ViewCount";

	static final int ANSWERS_S_SIZE = 1092420;

	static final int ANSWERS_ACCEPTED_SIZE = 8033996;

	private static final Logger LOGGER = Logger.getLogger(StackIndexer.class.getName());

	public static void main(String[] args) throws SQLException, IOException {
		new StackIndexer().indexSubsets(args[0], "stack_index/", ANSWERS_S_SIZE, "answers_s");
	}

	void indexSubsets(String experimentNumber, String indexFolderName, int tableSize, String tableName)
			throws SQLException, IOException {
		// setting up database connections
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		conn.setAutoCommit(false);
		// configuring index writer
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		config.setSimilarity(new BM25Similarity());
		config.setRAMBufferSizeMB(1024);
		File indexFile = new File("/data/ghadakcv/" + indexFolderName + experimentNumber);
		if (!indexFile.exists()) {
			indexFile.mkdir();
		}
		Directory directory = FSDirectory.open(Paths.get(indexFile.getAbsolutePath()));
		int limit = (int) (Double.parseDouble(experimentNumber) * tableSize / 100.0);
		// indexing
		LOGGER.log(Level.INFO, "indexing..");
		long start = System.currentTimeMillis();
		try (IndexWriter iwriter = new IndexWriter(directory, config)) {
			String query = "select Id, Body, ViewCount from stack_overflow." + tableName
					+ " order by ViewCount desc limit " + limit + ";";
			try (Statement stmt = conn.createStatement()) {
				stmt.setFetchSize(Integer.MIN_VALUE);
				ResultSet rs = stmt.executeQuery(query);
				while (rs.next()) {
					String id = rs.getString("Id");
					String answer = rs.getString("Body");
					String viewCount = rs.getString("ViewCount");
					Document doc = new Document();
					doc.add(new StoredField(ID_FIELD, id));
					doc.add(new StoredField(VIEW_COUNT_FIELD, viewCount));
					doc.add(new TextField(BODY_FIELD, answer, Store.NO));
					iwriter.addDocument(doc);
				}
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		long end = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "indexing time: {0} mins", (end - start) / 60000);
		dc.closeConnection();
	}

	void indexAllDataset() throws IOException, SQLException {
		// setting up database connections
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		conn.setAutoCommit(false);

		// configuring index writer
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		config.setSimilarity(new BM25Similarity());
		config.setRAMBufferSizeMB(1024);
		File indexFolder = new File("/data/ghadakcv/stack_fullindex");
		if (!indexFolder.exists()) {
			indexFolder.mkdir();
		}
		Directory directory = FSDirectory.open(Paths.get(indexFolder.getAbsolutePath()));
		// indexing
		LOGGER.log(Level.INFO, "indexing..");
		long start = System.currentTimeMillis();
		try (IndexWriter iwriter = new IndexWriter(directory, config)) {
			String query = "select Id, Body from stack_overflow.answers;";
			try (Statement stmt = conn.createStatement()) {
				stmt.setFetchSize(Integer.MIN_VALUE);
				ResultSet rs = stmt.executeQuery(query);
				while (rs.next()) {
					String id = rs.getString("Id");
					String answer = rs.getString("Body");
					Document doc = new Document();
					doc.add(new StoredField(ID_FIELD, id));
					doc.add(new TextField(BODY_FIELD, answer, Store.NO));
					iwriter.addDocument(doc);
				}
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		long end = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "indexing time: {0} mins", (end - start) / 60000);
		dc.closeConnection();
	}

}
