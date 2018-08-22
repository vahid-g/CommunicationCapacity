package irstyle;

import java.io.IOException;
import java.nio.file.Path;
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

public class BuildIndexForML {
	public static void main(String[] args) throws SQLException, IOException {
		List<String> argsList = Arrays.asList(args);
		String suffix;
		int[] limit;
		if (argsList.contains("-inexp")) {
			limit = ExperimentConstants.precisionLimit;
			suffix = "p20";
		} else if (argsList.contains("-inexr")) {
			limit = ExperimentConstants.recallLimit;
			suffix = "rec";
		} else {
			limit = ExperimentConstants.mrrLimit;
			suffix = "mrr";
		}
		Analyzer analyzer;
		if (argsList.contains("-bi")) {
			suffix += "_bi";
			analyzer = new BiwordAnalyzer();
		} else {
			analyzer = new StandardAnalyzer();
		}
		String finalSuffix = suffix;
		String[] tableName = { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
		String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
		int[] tableIndex = { 0, 1, 2 };
		Arrays.stream(tableIndex).parallel().forEach(tableNo -> {
			Path cacheIndexPath = Paths
					.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + tableName[tableNo] + "_cache_" + finalSuffix);
			Path restIndexPath = Paths
					.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + tableName[tableNo] + "_rest_" + finalSuffix);
			try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA);
					Directory cacheIndexDir = FSDirectory.open(cacheIndexPath);
					Directory restIndexDir = FSDirectory.open(restIndexPath);) {
				IndexWriterConfig cacheIndexWriterConfig = Indexer.getIndexWriterConfig(analyzer)
						.setOpenMode(OpenMode.CREATE);
				IndexWriterConfig restIndexWriterConfig = Indexer.getIndexWriterConfig(analyzer)
						.setOpenMode(OpenMode.CREATE);
				try (IndexWriter cacheIndexWriter = new IndexWriter(cacheIndexDir, cacheIndexWriterConfig);
						IndexWriter restIndexWriter = new IndexWriter(restIndexDir, restIndexWriterConfig)) {
					Indexer.indexTableAttribs(dc, cacheIndexWriter, tableName[tableNo], textAttribs[tableNo],
							limit[tableNo], "popularity", false);
					Indexer.indexTableAttribs(dc, restIndexWriter, tableName[tableNo], textAttribs[tableNo],
							ExperimentConstants.size[tableNo] - limit[tableNo], "popularity", true);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		analyzer.close();
	}

}
