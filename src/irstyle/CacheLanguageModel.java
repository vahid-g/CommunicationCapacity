package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import cache_selection_ml.FeatureExtraction;
import database.DatabaseConnection;
import wiki13.WikiFileIndexer;

public class CacheLanguageModel {

	static void indexForLM(DatabaseConnection dc, int[] limit, String suffix) throws IOException, SQLException {
		String[] tableNames = new String[] { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
		String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
		int[] sizes = { 11945034, 1183070, 9766351 };
		String[] popularity = { "popularity", "popularity", "popularity" };
		System.out.println("indexing cache LM..");
		Directory directory = FSDirectory.open(Paths.get(Indexer.DATA_WIKIPEDIA + "lm_cache_" + suffix));
		IndexWriterConfig config = Indexer.getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE);
		try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
			for (int i = 0; i < tableNames.length; i++) {
				Indexer.indexTable(dc, indexWriter, tableNames[i], textAttribs[i], limit[i], popularity[i], false);
			}
		}
		System.out.println("indexing comp LM..");
		directory = FSDirectory.open(Paths.get(Indexer.DATA_WIKIPEDIA + "lm_rest_" + suffix));
		config = Indexer.getIndexWriterConfig();
		config.setOpenMode(OpenMode.CREATE);
		try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
			for (int i = 0; i < tableNames.length; i++) {
				Indexer.indexTable(dc, indexWriter, tableNames[i], textAttribs[i], sizes[i] - limit[i], popularity[i],
						true);
			}
		}
	}

	static boolean useCache(String query, IndexReader cacheIndexReader, IndexReader globalIndexReader,
			IndexReader restIndexReader) throws IOException {
		System.out.println("  chacking cache selection..");
		FeatureExtraction fe = new FeatureExtraction(WikiFileIndexer.WEIGHT_ATTRIB);
		double ql_cache = 0;
		double ql_rest = 0;
		ql_cache = fe.queryLikelihood(cacheIndexReader, query, Indexer.TEXT_FIELD, globalIndexReader,
				new StandardAnalyzer());
		ql_rest = fe.queryLikelihood(restIndexReader, query, Indexer.TEXT_FIELD, globalIndexReader,
				new StandardAnalyzer());
		return (ql_cache >= ql_rest);
		// return false;
	}

}
