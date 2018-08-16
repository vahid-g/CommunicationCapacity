package irstyle;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import database.DatabaseConnection;
import database.DatabaseType;

public class BuildCache {
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

		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			// building the cache
			for (int i = 0; i < ExperimentConstants.tableName.length; i++) {
				System.out.println("Indexing table " + ExperimentConstants.tableName[i]);
				String cacheName = "sub_" + ExperimentConstants.tableName[i].substring(4) + "_" + suffix;
				buildCacheTable(dc, ExperimentConstants.tableName[i], ExperimentConstants.textAttribs[i], cacheName,
						limit[i]);
				buildCacheIndex(dc, ExperimentConstants.tableName[i], ExperimentConstants.textAttribs[i], cacheName,
						limit[i]);
			}
			System.out.println("finished building cache");
		}
	}

	private static void buildCacheTable(DatabaseConnection dc, String tableName, String[] textAttribs, String cacheName,
			int limit) throws SQLException, IOException {
		String selectStatement = "SELECT * FROM " + tableName + " ORDER BY popularity LIMIT " + limit;
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
	}

	private static void buildCacheIndex(DatabaseConnection dc, String tableName, String[] textAttribs, String cacheName,
			int limit) throws IOException, SQLException {
		try (Analyzer analyzer = new StandardAnalyzer()) {
			IndexWriterConfig config = RelationalWikiIndexer.getIndexWriterConfig(analyzer);
			config.setOpenMode(OpenMode.CREATE);
			RelationalWikiIndexer.indexTable(dc, RelationalWikiIndexer.DATA_WIKIPEDIA + cacheName, tableName,
					textAttribs, limit, "popularity", false, config);
		}
	}
}
