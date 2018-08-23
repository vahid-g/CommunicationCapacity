package irstyle;

import java.io.IOException;
import java.sql.SQLException;

import database.DatabaseConnection;
import irstyle.api.DatabaseHelper;
import irstyle.api.Indexer;

public class WikiIndexer {

	public static final String DATA_WIKIPEDIA = "/data/ghadakcv/wikipedia/";
	public static final String ID_FIELD = Indexer.idField;
	public static final String TEXT_FIELD = Indexer.textField;
	public static final String WEIGHT_FIELD = Indexer.weightField;

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
			Indexer.indexTable(dc, indexPath, tableName, new String[] { "url" }, limit, "pop", false,
					Indexer.getIndexWriterConfig());
		}
	}

	public static void indexImages(DatabaseConnection dc) throws IOException, SQLException {
		String tableName = "tbl_image_pop";
		for (int i = 1; i <= 100; i += 1) {
			double count = DatabaseHelper.tableSize(tableName, dc.getConnection());
			int limit = (int) Math.floor((i * count) / 100.0);
			String indexPath = DATA_WIKIPEDIA + tableName + "/" + i;
			Indexer.indexTable(dc, indexPath, tableName, new String[] { "src" }, limit, "pop", false,
					Indexer.getIndexWriterConfig());
		}
	}

}
