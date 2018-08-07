package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;

public class BuildCache_V2 {
	public static void main(String[] args) throws SQLException, IOException {
		List<String> argsList = Arrays.asList(args);
		// best inex p20 sizes with v2
		int[] precisionLimit = { 119450, 94640, 97663 };
		// best inex recall sizes wth v2
		int[] recallLimit = { 400000, 200000, 500000 };
		// best msn mrr sizes obtained with v2
		int[] mrrLimit = { 238900, 106470, 195326 };
		String suffix;
		int[] limit;
		if (argsList.contains("-p20")) {
			limit = precisionLimit;
			suffix = "p20";
		} else if (argsList.contains("-recall")) {
			limit = recallLimit;
			suffix = "rec";
		} else {
			limit = mrrLimit;
			suffix = "mrr";
		}
		Directory cacheDirectory = FSDirectory
				.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "lm_cache_" + suffix));
		IndexWriterConfig cacheConfig = RelationalWikiIndexer.getIndexWriterConfig().setOpenMode(OpenMode.CREATE);
		Directory restDirectory = FSDirectory
				.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "lm_rest_" + suffix));
		IndexWriterConfig restConfig = RelationalWikiIndexer.getIndexWriterConfig().setOpenMode(OpenMode.CREATE);
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA);
				IndexWriter cacheWriter = new IndexWriter(cacheDirectory, cacheConfig);
				IndexWriter restWriter = new IndexWriter(restDirectory, restConfig)) {
			String[] tableName = { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
			String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
			int[] size = { 11945034, 1183070, 9766351 };
			for (int i = 0; i < tableName.length; i++) {
				System.out.println("  indexing table " + tableName[i]);
				buildCacheTable(dc, tableName[i], textAttribs[i], limit[i], suffix);
				RelationalWikiIndexer.indexTable(dc, cacheWriter, tableName[i], textAttribs[i], limit[i], "popularity",
						false);
				RelationalWikiIndexer.indexTable(dc, restWriter, tableName[i], textAttribs[i], size[i] - limit[i],
						"popularity", true);
			}
		}
	}

	private static void buildCacheTable(DatabaseConnection dc, String tableName, String[] textAttribs, int limit,
			String suffix) throws SQLException, IOException {
		String selectStatement = "SELECT * FROM " + tableName + " ORDER BY popularity LIMIT " + limit;
		String cacheName = "sub_" + tableName.substring(4) + "_" + suffix;
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

		IndexWriterConfig config = RelationalWikiIndexer.getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE);
		RelationalWikiIndexer.indexTable(dc, RelationalWikiIndexer.DATA_WIKIPEDIA + cacheName, tableName, textAttribs,
				limit, "popularity", false, RelationalWikiIndexer.getIndexWriterConfig());
	}
}
