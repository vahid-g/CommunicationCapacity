package irstyle;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import database.DatabaseConnection;
import database.DatabaseType;

public class BuildCache_V2 {
	public static void main(String[] args) throws SQLException, IOException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			String[] tableName = { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
			String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
			// int[] limit = { 200000, 100000, 200000 };
			int[] limit = { 238900, 106470, 195326 };
			for (int i = 0; i < tableName.length; i++) {
				System.out.println("  indexing table " + tableName[i]);
				buildCache(dc, tableName[i], textAttribs[i], limit[i]);
			}
		}
	}

	private static void buildCache(DatabaseConnection dc, String tableName, String[] textAttribs, int limit)
			throws SQLException, IOException {
		String selectStatement = "SELECT * FROM " + tableName + " ORDER BY popularity LIMIT " + limit;
		String cacheName = "sub_" + tableName.substring(4);
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

		IndexWriterConfig config = IndexTable.getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE);
		IndexTable.indexTable(dc, IndexTable.DATA_WIKIPEDIA + cacheName, tableName, textAttribs, limit,
				"popularity", false, IndexTable.getIndexWriterConfig());
	}
}
