package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

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
		String[] tableNames = new String[] { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
		String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
		int[] sizes = { 11945034, 1183070, 9766351 };
		String[] popularity = { "popularity", "popularity", "popularity" };
		System.out.println("indexing cache LM..");
		Directory directory = FSDirectory.open(Paths.get(Indexer.DATA_WIKIPEDIA + "lm"));
		IndexWriterConfig config = Indexer.getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE);
		try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
			for (int i = 0; i < tableNames.length; i++) {
				Indexer.indexTable(dc, indexWriter, tableNames[i], textAttribs[i], limit[i], popularity[i], false);
			}
		}
		System.out.println("indexing comp LM..");
		directory = FSDirectory.open(Paths.get(Indexer.DATA_WIKIPEDIA + "lm_comp"));
		config = Indexer.getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE);
		try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
			for (int i = 0; i < tableNames.length; i++) {
				Indexer.indexTable(dc, indexWriter, tableNames[i], textAttribs[i], sizes[i] - limit[i], popularity[i],
						true);
			}
		}
	}

}
