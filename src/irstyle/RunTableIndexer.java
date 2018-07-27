package irstyle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;

public class RunTableIndexer {

	Connection conn;

	public RunTableIndexer(Analyzer analyzer, DatabaseConnection dc) throws IOException, SQLException {

		conn = dc.getConnection();
		conn.setAutoCommit(false);
	}

	public IndexWriterConfig getIndexWriterConfig() {
		IndexWriterConfig config;
		config = new IndexWriterConfig(new StandardAnalyzer());
		config.setSimilarity(new BM25Similarity());
		config.setRAMBufferSizeMB(1024);
		return config;
	}

	public static void main(String[] args) throws IOException, SQLException {
		if (args[0].equals("articles")) {
			RunTableIndexer.indexArticles();
		} else if (args[0].equals("images")) {
			RunTableIndexer.indexImages();
		} else if (args[0].equals("links")) {
			RunTableIndexer.indexLinks();
		} else if (args[0].equals("rest")) {
			RunTableIndexer.indexCompTable("tbl_article_09", 3, new String[] { "title", "text" }, "popularity");
			RunTableIndexer.indexCompTable("tbl_link_pop", 6, new String[] { "url" }, "pop");
			RunTableIndexer.indexCompTable("tbl_image_pop", 10, new String[] { "src" }, "pop");
			RunTableIndexer.indexCompTable("tbl_article_wiki13", 1, new String[] { "title", "text" }, "popularity");
		} else {
			System.out.println("Wrong input args!");
		}
	}

	public static void indexLinks() throws IOException, SQLException {
		String tableName = "tbl_link_pop";
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			RunTableIndexer wti = new RunTableIndexer(new StandardAnalyzer(), dc);
			for (int i = 1; i <= 100; i += 1) {
				double count = DatabaseHelper.tableSize(tableName, wti.conn);
				int limit = (int) Math.floor((i * count) / 100.0);
				String indexPath = "/data/ghadakcv/wikipedia/" + tableName + "/" + i;
				wti.indexTable(indexPath, tableName, "id", new String[] { "url" }, limit, "pop", false);
			}
		}
	}

	public static void indexImages() throws IOException, SQLException {
		String tableName = "tbl_image_pop";
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			RunTableIndexer wti = new RunTableIndexer(new StandardAnalyzer(), dc);
			for (int i = 1; i <= 100; i += 1) {
				double count = DatabaseHelper.tableSize(tableName, wti.conn);
				int limit = (int) Math.floor((i * count) / 100.0);
				String indexPath = "/data/ghadakcv/wikipedia/" + tableName + "/" + i;
				wti.indexTable(indexPath, tableName, "id", new String[] { "src" }, limit, "pop", false);
			}
		}
	}

	public static void indexArticles() throws IOException, SQLException {
		String tableName = "tbl_article_wiki13";
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			RunTableIndexer wti = new RunTableIndexer(new StandardAnalyzer(), dc);
			for (int i = 1; i <= 100; i += 1) {
				double count = DatabaseHelper.tableSize(tableName, wti.conn);
				int limit = (int) Math.floor((i * count) / 100.0);
				String indexPath = "/data/ghadakcv/wikipedia/" + tableName + "/" + i;
				wti.indexTable(indexPath, tableName, "id", new String[] { "title", "text" }, limit, "popularity",
						false);
			}
		}
	}

	public static void indexCompTable(String tableName, int percentage, String[] textAttribs, String popularityAttrib)
			throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			RunTableIndexer wti = new RunTableIndexer(new StandardAnalyzer(), dc);
			String indexPath = "/data/ghadakcv/wikipedia/" + tableName + "/c" + percentage;
			double count = DatabaseHelper.tableSize(tableName, wti.conn);
			int limit = (int) Math.floor(count - ((percentage * count) / 100.0));
			wti.indexTable(indexPath, tableName, "id", textAttribs, limit, popularityAttrib, true);
		}
	}

	private void indexTable(String indexPath, String table, String idAttrib, String[] textAttribs, int limit,
			String popularity, boolean ascending) throws IOException, SQLException {
		File indexFile = new File(indexPath);
		if (!indexFile.exists()) {
			indexFile.mkdirs();
		}
		Directory directory = FSDirectory.open(Paths.get(indexFile.getAbsolutePath()));
		try (IndexWriter iwriter = new IndexWriter(directory, getIndexWriterConfig())) {
			try (Statement stmt = conn.createStatement()) {
				stmt.setFetchSize(Integer.MIN_VALUE);
				String attribs = idAttrib;
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
					IndexerHelper.indexRS(idAttrib, textAttribs, iwriter, rs);
				}
			}
		}
	}

}
