package irstyle;

import java.io.IOException;
import java.sql.SQLException;

import database.DatabaseConnection;
import database.DatabaseType;
import irstyle.api.Indexer;

public class RelationalWikiIndexer {

	public static final String DATA_WIKIPEDIA = "/data/ghadakcv/wikipedia/";
	public static final String ID_FIELD = "id";
	public static final String TEXT_FIELD = "text";
	public static final String WEIGHT_FIELD = "weight";

	public static void main(String[] args) throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			if (args[0].equals("articles")) {
				RelationalWikiIndexer.indexArticles(dc);
			} else if (args[0].equals("images")) {
				RelationalWikiIndexer.indexImages(dc);
			} else if (args[0].equals("links")) {
				RelationalWikiIndexer.indexLinks(dc);
			} else if (args[0].equals("rest")) {
				Indexer.indexCompTable(dc, "tbl_article_09", 3, new String[] { "title", "text" }, "popularity");
				Indexer.indexCompTable(dc, "tbl_link_pop", 6, new String[] { "url" }, "pop");
				Indexer.indexCompTable(dc, "tbl_image_pop", 10, new String[] { "src" }, "pop");
				Indexer.indexCompTable(dc, "tbl_article_wiki13", 1, new String[] { "title", "text" }, "popularity");
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
			Indexer.indexTable(dc, indexPath, tableName, new String[] { "title", "text" }, limit, "popularity", false,
					Indexer.getIndexWriterConfig());
		}
	}

	public static void indexLinks(DatabaseConnection dc) throws IOException, SQLException {
		String tableName = "tbl_link_pop";
		for (int i = 1; i <= 100; i += 1) {
			double count = DatabaseHelper.tableSize(tableName, dc.getConnection());
			int limit = (int) Math.floor((i * count) / 100.0);
			String indexPath = DATA_WIKIPEDIA + tableName + "/" + i;
			Indexer.indexTable(dc, indexPath, tableName, new String[] { "url" }, limit, "pop", false, Indexer.getIndexWriterConfig());
		}
	}

	public static void indexImages(DatabaseConnection dc) throws IOException, SQLException {
		String tableName = "tbl_image_pop";
		for (int i = 1; i <= 100; i += 1) {
			double count = DatabaseHelper.tableSize(tableName, dc.getConnection());
			int limit = (int) Math.floor((i * count) / 100.0);
			String indexPath = DATA_WIKIPEDIA + tableName + "/" + i;
			Indexer.indexTable(dc, indexPath, tableName, new String[] { "src" }, limit, "pop", false, Indexer.getIndexWriterConfig());
		}
	}

}
