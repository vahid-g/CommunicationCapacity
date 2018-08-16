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

public class BuildIndexForML {
	public static void main(String[] args) throws SQLException, IOException {
		int tableNo = Integer.parseInt(args[0]); // this parameter is to select the table
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

			String table = tableName[tableNo];
			IndexWriterConfig config = RelationalWikiIndexer.getIndexWriterConfig(analyzer)
					.setOpenMode(OpenMode.CREATE);
			Directory directory = FSDirectory
					.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + table + "_cache_" + suffix));
			try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
				RelationalWikiIndexer.indexTableAttribs(dc, indexWriter, table, textAttribs[tableNo], limit[tableNo],
						"popularity", false);
			}
			config = RelationalWikiIndexer.getIndexWriterConfig(analyzer).setOpenMode(OpenMode.CREATE);
			directory = FSDirectory
					.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + table + "_rest_" + suffix));
			try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
				RelationalWikiIndexer.indexTableAttribs(dc, indexWriter, table, textAttribs[tableNo],
						ExperimentConstants.size[tableNo] - limit[tableNo], "popularity", true);
			}
		}
		analyzer.close();
	}

}
