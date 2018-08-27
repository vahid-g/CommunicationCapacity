package irstyle;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import database.DatabaseConnection;
import database.DatabaseType;
import irstyle.api.Indexer;
import stackoverflow.irstyle.StackConstants;

public class BuildCache {
	public static void main(String[] args) throws Exception {
		String suffix;
		int[] limit;
		DatabaseType databaseType;
		String dataDir;
		if (args[0].equals("-inexp") || args[0].equals("-inexr") || args[0].equals("-mrr")) {
			databaseType = DatabaseType.WIKIPEDIA;
			dataDir = WikiConstants.WIKI_DATA_DIR;
		} else if (args[0].equals("-stack")) {
			databaseType = DatabaseType.STACKOVERFLOW;
			dataDir = StackConstants.DATA_STACK;
		} else {
			throw new Exception();
		}
		if (args[0].equals("-inexp")) {
			limit = WikiConstants.precisionLimit;
			suffix = "p20";
		} else if (args[0].equals("-inexr")) {
			limit = WikiConstants.recallLimit;
			suffix = "rec";
		} else if (args[0].equals("-mrr")) {
			limit = WikiConstants.mrrLimit;
			suffix = "mrr";
		} else if (args[0].equals("-stack")) {
			limit = StackConstants.cacheSize;
			suffix = "mrr";
		} else {
			throw new Exception();
		}
		try (DatabaseConnection dc = new DatabaseConnection(databaseType)) {
			// building the cache
			for (int i = 0; i < WikiConstants.tableName.length; i++) {
				System.out.println("Indexing table " + WikiConstants.tableName[i]);
				String cacheName = "sub_" + WikiConstants.tableName[i].substring(4) + "_" + suffix;
				buildCacheTable(dc, WikiConstants.tableName[i], WikiConstants.textAttribs[i], cacheName, limit[i]);
				buildCacheIndex(dc, WikiConstants.tableName[i], WikiConstants.textAttribs[i], cacheName, limit[i],
						dataDir);
			}
			System.out.println("finished building cache");
		}
	}

	private static void buildCacheTable(DatabaseConnection dc, String tableName, String[] textAttribs, String cacheName,
			int limit) throws SQLException, IOException {
		String selectStatement = "SELECT * FROM " + tableName + " ORDER BY popularity desc LIMIT " + limit;
		String createStatement = "CREATE TABLE " + cacheName + " AS " + selectStatement + ";";
		System.out.println("Creating table..");
		System.out.println("sql: " + createStatement);
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.execute("drop table if exists " + cacheName);
			stmt.execute(createStatement);
		}
		System.out.println("Creating id index..");
		String createIndex = "CREATE INDEX id ON " + cacheName + "(id);";
		System.out.println("sql: " + createIndex);
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.executeUpdate(createIndex);
		}
	}

	private static void buildCacheIndex(DatabaseConnection dc, String tableName, String[] textAttribs, String cacheName,
			int limit, String dataDir) throws IOException, SQLException {
		try (Analyzer analyzer = new StandardAnalyzer()) {
			IndexWriterConfig config = Indexer.getIndexWriterConfig(analyzer);
			config.setOpenMode(OpenMode.CREATE);
			Indexer.indexTable(dc, dataDir + cacheName, tableName, textAttribs, limit, "popularity", false, config);
		}
	}
}
