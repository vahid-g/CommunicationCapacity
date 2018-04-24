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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringEscapeUtils;
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
import indexing.BiwordAnalyzer;

public class StackIndexer {

	public static final String ID_FIELD = "id";

	public static final String BODY_FIELD = "Body";

	public static final String VIEW_COUNT_FIELD = "ViewCount";

	static final int ANSWERS_S_SIZE = 1092420;

	static final int ANSWERS_AA_SIZE = 8033979;

	private static final Logger LOGGER = Logger.getLogger(StackIndexer.class.getName());

	public static void main(String[] args) throws SQLException, IOException {
		Options options = new Options();
		Option indexOption = new Option("index", true, "index number");
		indexOption.setRequired(true);
		options.addOption(indexOption);
		options.addOption(new Option("bi", false, "biword index"));
		options.addOption(new Option("rest", false, "rest index"));
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;
		try {
			cl = clp.parse(options, args);
			StackIndexer si = null;
			String indexBasePath = "";
			if (cl.hasOption("bi")) {
				si = new StackIndexer(new BiwordAnalyzer());
				indexBasePath = "/data/ghadakcv/stack_index_bi/";
			} else {
				si = new StackIndexer(new StandardAnalyzer());
				indexBasePath = "/data/ghadakcv/stack_index/";
			}
			String indexNumber = cl.getOptionValue("index");
			if (cl.hasOption("rest")) {
				String indexPath = indexBasePath + "c" + indexNumber;
				si.indexRest(indexNumber, indexPath, ANSWERS_S_SIZE, "answers_s");
			} else {
				String indexPath = indexBasePath + indexNumber;
				si.indexSubsets(indexNumber, indexPath, ANSWERS_S_SIZE, "answers_s");
			}
			// si.indexSubsets(index, "stack_index_aa/", ANSWERS_AA_SIZE, "answers_aa");
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			formatter.printHelp("", options);
			return;
		}
	}

	private IndexWriterConfig config;

	public StackIndexer(Analyzer analyzer) {
		config = new IndexWriterConfig(analyzer);
		config.setSimilarity(new BM25Similarity());
		config.setRAMBufferSizeMB(1024);
	}

	void indexSubsets(String experimentNumber, String indexPath, int tableSize, String tableName)
			throws SQLException, IOException {
		int limit = (int) (Double.parseDouble(experimentNumber) * tableSize / 100.0);
		// indexing
		LOGGER.log(Level.INFO, "indexing subset..");
		String query = "select Id, Body, ViewCount from stack_overflow." + tableName + " order by ViewCount desc limit "
				+ limit + ";";
		indexTable(indexPath, query);
	}

	void indexRest(String experimentNumber, String indexPath, int tableSize, String tableName)
			throws SQLException, IOException {
		int limit = (int) (tableSize - Double.parseDouble(experimentNumber) * tableSize / 100.0);
		// indexing
		LOGGER.log(Level.INFO, "indexing rest..");
		String query = "select Id, Body, ViewCount from stack_overflow." + tableName + " order by ViewCount asc limit "
				+ limit + ";";
		indexTable(indexPath, query);
	}

	private void indexTable(String indexPath, String query) throws IOException, SQLException {
		LOGGER.log(Level.INFO, "query: {0}", query);
		// setting up database connections
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		conn.setAutoCommit(false);
		File indexFile = new File(indexPath);
		if (!indexFile.exists()) {
			indexFile.mkdirs();
		}
		Directory directory = FSDirectory.open(Paths.get(indexFile.getAbsolutePath()));
		// indexing
		LOGGER.log(Level.INFO, "indexing..");
		long start = System.currentTimeMillis();
		try (IndexWriter iwriter = new IndexWriter(directory, config)) {
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
					answer = answer.replaceAll("<[^>]+>", " "); // remove xml tags
					answer = StringEscapeUtils.unescapeHtml4(answer); // convert html encoded characters to unicode
					// answer = answer.replaceAll("[^a-zA-Z0-9'. ]", " ").replaceAll("\\s+", " ");
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

	void indexAllAnswers() throws IOException, SQLException {
		// setting up database connections
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		conn.setAutoCommit(false);
		// configuring index writer
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
