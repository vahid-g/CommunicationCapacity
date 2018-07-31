package irstyle;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import database.DatabaseConnection;
import database.DatabaseType;

public class BuildLanguageModel {

	public static void main(String[] args) throws IOException, SQLException {
		int[] limit = { 238900, 106470, 195326 };
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			indexForLM(dc, limit);
		}
	}

	static void indexForLM(DatabaseConnection dc, int[] limit) throws IOException, SQLException {
		IndexWriterConfig config = Indexer.getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE);
		String indexPath = Indexer.DATA_WIKIPEDIA + "lm";
		String[] tableNames = new String[] { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
		String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
		int[] sizes = { 11945034, 1183070, 9766351 };
		String[] popularity = { "popularity", "popularity", "popularity" };
		System.out.println("indexing union..");
		for (int i = 0; i < tableNames.length; i++) {
			Indexer.indexTable(dc, indexPath, tableNames[i], textAttribs[i], limit[i], popularity[i], false, config);
		}
		System.out.println("indexing comp..");
		indexPath = Indexer.DATA_WIKIPEDIA + "lm_comp";
		config = Indexer.getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE);
		for (int i = 0; i < tableNames.length; i++) {
			Indexer.indexTable(dc, indexPath, tableNames[i], textAttribs[i], sizes[i] - limit[i], popularity[i], true,
					config);
		}
	}

}
