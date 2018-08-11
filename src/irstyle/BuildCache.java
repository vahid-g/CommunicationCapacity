package irstyle;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import database.DatabaseConnection;
import database.DatabaseType;

public class BuildCache {
	public static void main(String[] args) throws SQLException, IOException {
		List<String> argsList = Arrays.asList(args);
		// best inex p20 sizes with v2 1%, 8%, 1%
		int[] precisionLimit = { 119450, 94640, 97663 };
		// best inex recall sizes wth v2 3%, 16%, 55
		int[] recallLimit = { 400000, 200000, 500000 };
		// best msn mrr sizes obtained with v2
		int[] mrrLimit = { 238900, 106470, 195326 };
		String suffix;
		int[] limit;
		if (argsList.contains("-inexp")) {
			limit = precisionLimit;
			suffix = "p20";
		} else if (argsList.contains("-inexr")) {
			limit = recallLimit;
			suffix = "rec";
		} else {
			limit = mrrLimit;
			suffix = "mrr";
		}
		String[] tableName = { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
		String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			// building the cache
			for (int i = 0; i < tableName.length; i++) {
				System.out.println("Indexing table " + tableName[i]);
				String cacheName = "sub_" + tableName[i].substring(4) + "_" + suffix;
				buildCacheTable(dc, tableName[i], textAttribs[i], cacheName, limit[i]);
				buildCacheIndex(dc, tableName[i], textAttribs[i], cacheName, limit[i]);
			}
			System.out.println("finished building cache");
		}
	}

	private static void buildCacheTable(DatabaseConnection dc, String tableName, String[] textAttribs, String cacheName,
			int limit) throws SQLException, IOException {
		String selectStatement = "SELECT * FROM " + tableName + " ORDER BY popularity LIMIT " + limit;
		String createStatement = "CREATE TABLE " + cacheName + " AS " + selectStatement + ";";
		System.out.println("Creating table..");
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.execute("drop table if exists " + cacheName);
			stmt.execute(createStatement);
		}
		System.out.println("Creating id index..");
		String createIndex = "CREATE INDEX id ON " + cacheName + "(id);";
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.executeUpdate(createIndex);
		}
	}

	private static void buildCacheIndex(DatabaseConnection dc, String tableName, String[] textAttribs, String cacheName,
			int limit) throws IOException, SQLException {
		try (Analyzer analyzer = new StandardAnalyzer()) {
			IndexWriterConfig config = RelationalWikiIndexer.getIndexWriterConfig(analyzer);
			config.setOpenMode(OpenMode.CREATE);
			RelationalWikiIndexer.indexTable(dc, RelationalWikiIndexer.DATA_WIKIPEDIA + cacheName, tableName,
					textAttribs, limit, "popularity", false, config);
		}
	}
}
