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
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;

public class IndexTable {

	public static final String DATA_WIKIPEDIA = "/data/ghadakcv/wikipedia/";
	public static final String ID_FIELD = "id";
	public static final String TEXT_FIELD = "text";

	public static void main(String[] args) throws IOException, SQLException {
		// int[] limit = { 200000, 100000, 200000 };
		int[] limit = { 238900, 106470, 195326 };
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			if (args[0].equals("articles")) {
				IndexTable.indexArticles(dc);
			} else if (args[0].equals("images")) {
				IndexTable.indexImages(dc);
			} else if (args[0].equals("links")) {
				IndexTable.indexLinks(dc);
			} else if (args[0].equals("rest")) {
				indexCompTable(dc, "tbl_article_09", 3, new String[] { "title", "text" }, "popularity");
				indexCompTable(dc, "tbl_link_pop", 6, new String[] { "url" }, "pop");
				indexCompTable(dc, "tbl_image_pop", 10, new String[] { "src" }, "pop");
				indexCompTable(dc, "tbl_article_wiki13", 1, new String[] { "title", "text" }, "popularity");
			} else if (args[0].equals("union")) {
				indexForLM(dc, limit);
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

	static void indexForLM(DatabaseConnection dc, int[] limit) throws IOException, SQLException {
		IndexWriterConfig config = getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE_OR_APPEND);
		String indexPath = DATA_WIKIPEDIA + "union";
		String[] tableNames = new String[] { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
		String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
		int[] sizes = { 11945034, 1183070, 9766351 };
		String[] popularity = { "popularity", "popularity", "popularity" };
		System.out.println("indexing union..");
		for (int i = 0; i < tableNames.length; i++) {
			indexTable(dc, indexPath, tableNames[i], textAttribs[i], limit[i], popularity[i], false, config);
		}
		System.out.println("indexing comp..");
		indexPath = DATA_WIKIPEDIA + "union_comp";
		for (int i = 0; i < tableNames.length; i++) {
			indexTable(dc, indexPath, tableNames[i], textAttribs[i], sizes[i] - limit[i], popularity[i], true, config);
		}
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
					IndexTable.indexRS("id", textAttribs, iwriter, rs);
				}
			}
		}
	}

}
