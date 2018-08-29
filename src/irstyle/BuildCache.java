package irstyle;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import database.DatabaseConnection;
import irstyle.api.IRStyleExperiment;
import irstyle.api.Indexer;

public class BuildCache {
	public static void main(String[] args) throws Exception {
		IRStyleExperiment experiment;
		if (args[0].equals("-inexp")) {
			experiment = IRStyleExperiment.createWikiP20Experiment();
		} else if (args[0].equals("-inexr")) {
			experiment = IRStyleExperiment.createWikiRecExperiment();
		} else if (args[0].equals("-msn")) {
			experiment = IRStyleExperiment.createWikiMrrExperiment();
		} else if (args[0].equals("-stack")) {
			experiment = IRStyleExperiment.createStackExperiment();
		} else {
			throw new Exception();
		}
		try (DatabaseConnection dc = new DatabaseConnection(experiment.databaseType)) {
			// building the cache
			for (int i = 0; i < experiment.tableNames.length; i++) {
				System.out.println("Indexing table " + experiment.tableNames[i]);
				buildCacheTable(dc, experiment.tableNames[i], experiment.textAttribs[i], experiment.popularity,
						experiment.cacheNames[i], experiment.limits[i]);
				buildCacheIndex(dc, experiment.tableNames[i], experiment.textAttribs[i], experiment.popularity,
						experiment.cacheNames[i], experiment.limits[i], experiment.dataDir);
			}
			System.out.println("finished building cache");
		}
	}

	private static void buildCacheTable(DatabaseConnection dc, String tableName, String[] textAttribs,
			String popularity, String cacheName, int limit) throws SQLException, IOException {
		String selectStatement = "SELECT * FROM " + tableName + " ORDER BY " + popularity + "  desc LIMIT " + limit;
		String createStatement = "CREATE TABLE " + cacheName + " AS " + selectStatement + ";";
		System.out.println("Creating table..");
		System.out.println("sql: " + createStatement);
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.execute("drop table if exists " + cacheName);
			stmt.execute(createStatement);
		}
		System.out.println("Creating id index..");
		String createIndex = "CREATE INDEX id ON " + cacheName + "(id);";
		System.out.println("sql: " + createIndex);
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.executeUpdate(createIndex);
		}
	}

	private static void buildCacheIndex(DatabaseConnection dc, String tableName, String[] textAttribs,
			String popularity, String cacheName, int limit, String dataDir) throws IOException, SQLException {
		try (Analyzer analyzer = new StandardAnalyzer()) {
			IndexWriterConfig config = Indexer.getIndexWriterConfig(analyzer);
			config.setOpenMode(OpenMode.CREATE);
			Indexer.indexTable(dc, dataDir + cacheName, tableName, textAttribs, limit, popularity, false, config);
		}
	}
}
