package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;
import indexing.BiwordAnalyzer;

public class BuildSecondaryIndex {
	public static void main(String[] args) throws SQLException, IOException {
		List<String> argsList = Arrays.asList(args);
		// best inex p20 sizes with v2 1%, 8%, 1%
		int[] precisionLimit = { 119450, 94640, 97663 };
		// best inex recall sizes wth v2 3%, 16%, 55
		int[] recallLimit = { 400000, 200000, 500000 };
		// best msn mrr sizes obtained with v2
		int[] mrrLimit = { 238900, 106470, 195326 };
		int[] size = { 11945034, 1183070, 9766351 };
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
		Analyzer analyzer = null;
		if (argsList.contains("-bi")) {
			suffix += "_bi";
			analyzer = new BiwordAnalyzer();
		} else {
			analyzer = new StandardAnalyzer();
		}
		String[] tableName = { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
		String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			// building index for LM
			Directory cacheDirectory = FSDirectory
					.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "lm_cache_" + suffix));
			IndexWriterConfig cacheConfig = RelationalWikiIndexer.getIndexWriterConfig(analyzer)
					.setOpenMode(OpenMode.CREATE);
			Directory restDirectory = FSDirectory
					.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "lm_rest_" + suffix));
			Directory allDirectory = FSDirectory
					.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "lm_all_" + suffix));
			IndexWriterConfig restConfig = RelationalWikiIndexer.getIndexWriterConfig(analyzer)
					.setOpenMode(OpenMode.CREATE);
			IndexWriterConfig allConfig = RelationalWikiIndexer.getIndexWriterConfig(analyzer);
			try (IndexWriter cacheWriter = new IndexWriter(cacheDirectory, cacheConfig);
					IndexWriter restWriter = new IndexWriter(restDirectory, restConfig);
					IndexWriter allWriter = new IndexWriter(allDirectory, allConfig)) {
				for (int i = 0; i < tableName.length; i++) {
					System.out.println("Indexing table " + tableName[i]);
					RelationalWikiIndexer.indexTable(dc, cacheWriter, tableName[i], textAttribs[i], limit[i],
							"popularity", false);
					RelationalWikiIndexer.indexTable(dc, restWriter, tableName[i], textAttribs[i], size[i] - limit[i],
							"popularity", true);
					RelationalWikiIndexer.indexTable(dc, allWriter, tableName[i], textAttribs[i], size[i], "popularity",
							false);
				}
			}
		}
		analyzer.close();
	}
}
