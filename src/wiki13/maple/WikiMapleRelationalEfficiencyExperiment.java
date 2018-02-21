package wiki13.maple;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperimentHelper;
import wiki13.WikiFilesPaths;

public class WikiMapleRelationalEfficiencyExperiment {

	private static Logger LOGGER = Logger.getLogger(WikiMapleRelationalEfficiencyExperiment.class.getName());
	private static WikiFilesPaths PATHS = WikiFilesPaths.getMaplePaths();

	public static void main(String[] args) throws SQLException {
		LOGGER.setLevel(Level.INFO);
		queryEfficiencyExperiment(args[0]);
	}

	static void debug() {
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(PATHS.getMsnQueryFilePath(),
				PATHS.getMsnQrelFilePath());
		queries = queries.subList(0, 10);
		double[] tmp = new double[10];
		for (int j = 0; j < 2; j++) {
			for (int i = 91; i > 0; i -= 10) {
				String indexPath = PATHS.getIndexBase() + i;
				long startTime = System.currentTimeMillis();
				WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.1f);
				long spentTime = System.currentTimeMillis() - startTime;
				tmp[i / 10] = spentTime / queries.size();
			}
			LOGGER.log(Level.INFO, Arrays.toString(tmp));
		}
	}

	static void queryEfficiencyExperiment(String mode) {
		String subsetIndexPath = PATHS.getIndexBase() + "1";
		String indexPath = PATHS.getIndexBase() + "100";
		Properties config = new Properties();
		try (InputStream in = WikiMapleRelationalEfficiencyExperiment.class
				.getResourceAsStream("/config/config.properties")) {
			config.load(in);
			try (Connection con = getDatabaseConnection(config.get("username"), config.get("password"),
					config.get("db-url"))) {
				List<ExperimentQuery> queries = QueryServices.loadMsnQueries(PATHS.getMsnQueryFilePath(),
						PATHS.getMsnQrelFilePath());
				queries = queries.subList(0, 500);
				String subsetPrefix = "";
				switch (mode) {
				case "subset":
					subsetPrefix = "SELECT a.id FROM tmp_article a left join "
							+ "tmp_image i on a.id = i.article_id left join "
							+ " tmp_link l on a.id = l.article_id WHERE a.id in %s;";
					break;
				case "denorm":
					subsetPrefix = "SELECT a.id FROM tmp_article_denorm WHERE a.id in %s;";
					break;
				case "memory":
					subsetPrefix = "SELECT a.id FROM mem_article a left join "
							+ "mem_image i on a.id = i.article_id left join "
							+ "mem_image_image ii on i.image_id = ii.id left join "
							+ "mem_link l on a.id = l.article_id left join "
							+ "mem_link_link ll on l.article_id = ll.id WHERE a.id in %s;";
					break;
				default:
					LOGGER.log(Level.SEVERE, "Mode is not correct");
					return;
				}
				String dbPrefix = "SELECT a.id FROM tbl_article_wiki13 a left join "
						+ "tbl_article_image_09 i on a.id = i.article_id left join "
						+ "tbl_image_09 ii on i.image_id = ii.id left join "
						+ "tbl_article_link_09 l on a.id = l.article_id left join "
						+ "tbl_link_09 ll on l.link_id = ll.id WHERE a.id in %s;";
				double[] time = new double[4];
				int iterCount = 3;
				for (int i = 0; i < iterCount; i++) {
					double dbTimes[] = measureQueryEfficiency(indexPath, con, queries, dbPrefix);
					double subsetTimes[] = measureQueryEfficiency(subsetIndexPath, con, queries, subsetPrefix);
					time[0] += subsetTimes[0];
					time[1] += subsetTimes[1];
					time[2] += dbTimes[0];
					time[3] += dbTimes[1];
				}
				LOGGER.log(Level.INFO, "Average per query time (ms) after " + iterCount + " interations:");
				LOGGER.log(Level.INFO, "subset: " + time[0] / iterCount + "," + time[1] / iterCount + ","
						+ (time[0] + time[1]) / iterCount);
				LOGGER.log(Level.INFO, "db: " + time[2] / iterCount + "," + time[3] / iterCount + ","
						+ (time[2] + time[3]) / iterCount);
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	static double[] measureQueryEfficiency(String indexPath, Connection con, List<ExperimentQuery> queries,
			String queryPrefix) throws SQLException {
		long startTime = System.currentTimeMillis();
		List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 1f);
		long middleTime = System.currentTimeMillis();
		int counter = 0;
		for (QueryResult result : results) {
			List<String> ids = result.getTopDocuments().subList(0, Math.min(result.getTopDocuments().size(), 20))
					.stream().map(t -> t.id).collect(Collectors.toList());
			String query = String.format(queryPrefix, ids.toString().replace('[', '(').replace(']', ')'));
			long tmp = System.currentTimeMillis();
			long queryTime = submitSqlQuery(con, query);
			LOGGER.log(Level.FINE, counter++ + ": " + ids.size() + ": " + (System.currentTimeMillis() - tmp) + ": "
					+ ": " + queryTime + ": " + query);
		}
		long endTime = System.currentTimeMillis();
		return new double[] { (middleTime - startTime) / queries.size(), (endTime - middleTime) / queries.size() };
	}

	static long submitSqlQuery(Connection con, String query) throws SQLException {
		try (Statement stmt = con.createStatement()) {
			long begin = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(query);
			long end = System.currentTimeMillis();
			int counter = 0;
			while (rs.next()) {
				counter++;
				rs.getString("id");
			}
			LOGGER.log(Level.FINE, "fetch size: " + counter);
			return end - begin;
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return -1;
	}

	static Connection getDatabaseConnection(Object user, Object password, Object dbUrl) throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", password);
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(dbUrl.toString(), connectionProps);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		LOGGER.log(Level.INFO, "Successfully connected to db");
		return conn;
	}

}
