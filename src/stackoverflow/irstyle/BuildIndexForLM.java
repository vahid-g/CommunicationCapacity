package stackoverflow.irstyle;

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
import irstyle.api.Indexer;

public class BuildIndexForLM {

	public static void main(String[] args) throws SQLException, IOException {
		List<String> argsList = Arrays.asList(args);
		String suffix = "mrr";
		int[] limit = Constants.cacheSize;
		Analyzer analyzer = null;
		if (argsList.contains("-bi")) {
			suffix += "_bi";
			analyzer = new BiwordAnalyzer();
		} else {
			analyzer = new StandardAnalyzer();
		}
		String[] tableName = Constants.tableName;
		String[][] textAttribs = Constants.textAttribs;
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW)) {
			// building index for LM
			Directory cacheDirectory = FSDirectory.open(Paths.get(Constants.DATA_STACK + "lm_cache_" + suffix));
			IndexWriterConfig cacheConfig = Indexer.getIndexWriterConfig(analyzer).setOpenMode(OpenMode.CREATE);
			Directory restDirectory = FSDirectory.open(Paths.get(Constants.DATA_STACK + "lm_rest_" + suffix));
			Directory allDirectory = FSDirectory.open(Paths.get(Constants.DATA_STACK + "lm_all_" + suffix));
			IndexWriterConfig restConfig = Indexer.getIndexWriterConfig(analyzer).setOpenMode(OpenMode.CREATE);
			IndexWriterConfig allConfig = Indexer.getIndexWriterConfig(analyzer);
			try (IndexWriter cacheWriter = new IndexWriter(cacheDirectory, cacheConfig);
					IndexWriter restWriter = new IndexWriter(restDirectory, restConfig);
					IndexWriter allWriter = new IndexWriter(allDirectory, allConfig)) {
				for (int i = 0; i < tableName.length; i++) {
					System.out.println("Indexing table " + tableName[i]);
					Indexer.indexTable(dc, cacheWriter, tableName[i], textAttribs[i], limit[i], "ViewCount", false);
					Indexer.indexTable(dc, restWriter, tableName[i], textAttribs[i], Constants.size[i] - limit[i],
							"ViewCount", true);
					// Indexer.indexTable(dc, allWriter, tableName[i], textAttribs[i],
					// Constants.size[i], "popularity", false);
				}
			}
		}
		analyzer.close();
	}
}
