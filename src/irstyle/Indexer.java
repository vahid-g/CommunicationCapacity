package irstyle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

public class Indexer {

	public static final String DATA_WIKIPEDIA = "/data/ghadakcv/wikipedia/";
	public static final String ID_FIELD = "id";
	public static final String TEXT_FIELD = "text";

	public static void main(String[] args) throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			if (args[0].equals("articles")) {
				Indexer.indexArticles(dc);
			} else if (args[0].equals("images")) {
				Indexer.indexImages(dc);
			} else if (args[0].equals("links")) {
				Indexer.indexLinks(dc);
			} else if (args[0].equals("rest")) {
				indexCompTable(dc, "tbl_article_09", 3, new String[] { "title", "text" }, "popularity");
				indexCompTable(dc, "tbl_link_pop", 6, new String[] { "url" }, "pop");
				indexCompTable(dc, "tbl_image_pop", 10, new String[] { "src" }, "pop");
				indexCompTable(dc, "tbl_article_wiki13", 1, new String[] { "title", "text" }, "popularity");
			} else {
				System.out.println("Wrong input args!");
			}
		}
	}

	public static void indexArticles(DatabaseConnection dc) throws IOException, SQLException {
		String tableName = "tbl_article_wiki13";
		for (int i = 1; i <= 100; i += 1) {
			double count = DatabaseHelper.tableSize(tableName, dc.getConnection());
			int limit = (int) Math.floor((i * count) / 100.0);
			String indexPath = DATA_WIKIPEDIA + tableName + "/" + i;
			indexTable(dc, indexPath, tableName, new String[] { "title", "text" }, limit, "popularity", false,
					getIndexWriterConfig());
		}
	}

	public static void indexLinks(DatabaseConnection dc) throws IOException, SQLException {
		String tableName = "tbl_link_pop";
		for (int i = 1; i <= 100; i += 1) {
			double count = DatabaseHelper.tableSize(tableName, dc.getConnection());
			int limit = (int) Math.floor((i * count) / 100.0);
			String indexPath = DATA_WIKIPEDIA + tableName + "/" + i;
			indexTable(dc, indexPath, tableName, new String[] { "url" }, limit, "pop", false, getIndexWriterConfig());
		}
	}

	public static void indexImages(DatabaseConnection dc) throws IOException, SQLException {
		String tableName = "tbl_image_pop";
		for (int i = 1; i <= 100; i += 1) {
			double count = DatabaseHelper.tableSize(tableName, dc.getConnection());
			int limit = (int) Math.floor((i * count) / 100.0);
			String indexPath = DATA_WIKIPEDIA + tableName + "/" + i;
			indexTable(dc, indexPath, tableName, new String[] { "src" }, limit, "pop", false, getIndexWriterConfig());
		}
	}

	public static void indexCompTable(DatabaseConnection dc, String tableName, int percentage, String[] textAttribs,
			String popularityAttrib) throws IOException, SQLException {
		String indexPath = DATA_WIKIPEDIA + tableName + "/c" + percentage;
		double count = DatabaseHelper.tableSize(tableName, dc.getConnection());
		int limit = (int) Math.floor(count - ((percentage * count) / 100.0));
		indexTable(dc, indexPath, tableName, textAttribs, limit, popularityAttrib, true, getIndexWriterConfig());
	}

	public static void indexRS(String idAttrib, String[] textAttribs, IndexWriter iwriter, ResultSet rs)
			throws SQLException, IOException {
		String id = rs.getString(idAttrib);
		StringBuilder answerBuilder = new StringBuilder();
		for (String s : textAttribs) {
			answerBuilder.append(rs.getString(s));
		}
		String answer = answerBuilder.toString();
		Document doc = new Document();
		doc.add(new StoredField(ID_FIELD, id));
		// answer = StringEscapeUtils.unescapeHtml4(answer); // convert html encoded
		// characters to unicode
		doc.add(new TextField(TEXT_FIELD, answer, Store.NO));
		iwriter.addDocument(doc);
	}

	static IndexWriterConfig getIndexWriterConfig() {
		IndexWriterConfig config;
		config = new IndexWriterConfig(new StandardAnalyzer());
		config.setSimilarity(new BM25Similarity());
		config.setRAMBufferSizeMB(1024);
		return config;
	}

	static void indexTable(DatabaseConnection dc, String indexPath, String table, String[] textAttribs, int limit,
			String popularity, boolean ascending, IndexWriterConfig config) throws IOException, SQLException {
		File indexFile = new File(indexPath);
		if (!indexFile.exists()) {
			indexFile.mkdirs();
		}
		Directory directory = FSDirectory.open(Paths.get(indexFile.getAbsolutePath()));
		try (IndexWriter iwriter = new IndexWriter(directory, config)) {
			try (Statement stmt = dc.getConnection().createStatement()) {
				stmt.setFetchSize(Integer.MIN_VALUE);
				String attribs = "id";
				for (String s : textAttribs) {
					attribs += "," + s;
				}
				String sql = "select " + attribs + " from " + table + " order by " + popularity + " desc limit " + limit
						+ ";";
				if (ascending) {
					sql = "select " + attribs + " from " + table + " order by " + popularity + " asc limit " + limit
							+ ";";
				}
				System.out.println(sql);
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					Indexer.indexRS("id", textAttribs, iwriter, rs);
				}
			}
		}
	}

	static void indexTable(DatabaseConnection dc, IndexWriter indexWriter, String table, String[] textAttribs,
			int limit, String popularity, boolean ascending) throws IOException, SQLException {
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			String attribs = "id";
			for (String s : textAttribs) {
				attribs += "," + s;
			}
			String sql = "select " + attribs + " from " + table + " order by " + popularity + " desc limit " + limit
					+ ";";
			if (ascending) {
				sql = "select " + attribs + " from " + table + " order by " + popularity + " asc limit " + limit + ";";
			}
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				Indexer.indexRS("id", textAttribs, indexWriter, rs);
			}
		}
	}

}
