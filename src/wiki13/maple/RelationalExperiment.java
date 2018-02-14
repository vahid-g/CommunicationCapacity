package wiki13.maple;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperimentHelper;

public class RelationalExperiment {

	public static void main(String[] args) throws SQLException {
		queryEfficiencyExperiment();
	}

	static void debug() {
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(WikiMaplePaths.MSN_QUERY_FILE_PATH,
				WikiMaplePaths.MSN_QREL_FILE_PATH);
		queries = queries.subList(0, 100);
		double[] tmp = new double[10];
		for (int j = 0; j < 3; j++) {
			for (int i = 91; i > 0; i -= 10) {
				String indexPath = WikiMaplePaths.INDEX_BASE + i;
				long startTime = System.currentTimeMillis();
				WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.1f);
				long spentTime = System.currentTimeMillis() - startTime;
				tmp[i / 10] = spentTime / queries.size();
			}
			System.out.println(Arrays.toString(tmp));
		}
	}

	static void queryEfficiencyExperiment() {
		// int docsInSubset = 1163610;
		String subsetIndexPath = WikiMaplePaths.DATA_PATH + "wiki_index/1";
		String indexPath = WikiMaplePaths.DATA_PATH + "wiki_index/100";
		Properties config = new Properties();
		try (InputStream in = RelationalExperiment.class.getResourceAsStream("/config/config.properties")) {
			config.load(in);
			try (Connection con = getDatabaseConnection(config.get("username"), config.get("password"),
					config.get("db-url"))) {
				List<ExperimentQuery> queries = QueryServices.loadMsnQueries(WikiMaplePaths.MSN_QUERY_FILE_PATH,
						WikiMaplePaths.MSN_QREL_FILE_PATH);
				queries = queries.subList(0, 100);

//				String subsetPrefix = "SELECT a.id FROM tmp_article_1 a left join "
//						+ "tmp_article_image_1 i on a.id = i.article_id left join "
//						+ "tmp_article_link_1 l on a.id= l.article_id WHERE a.id in ";
//				String dbPrefix = "SELECT a.id FROM tbl_article_wiki13 a left join "
//						+ "tbl_article_image_09 i on a.id = i.article_id left join "
//						+ "tbl_article_link_09 l on a.id= l.article_id WHERE a.id in ";
				String subsetPrefix = "SELECT a.id FROM tmp_article_1 a left join "
						+ "tbl_article_link_09 l on a.id= l.article_id WHERE a.id in %s;";
				String dbPrefix = "SELECT a.id FROM tbl_article_wiki13 a left join "
						+ "tbl_article_link_09 l on a.id= l.article_id  WHERE a.id in %s;";

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
				System.out.println("Average per query time (ms) after " + iterCount + " interations:");
				System.out.println("subset: " + time[0] / iterCount + "," + time[1] / iterCount + ","
						+ (time[0] + time[1]) / iterCount);
				System.out.println("db: " + time[2] / iterCount + "," + time[3] / iterCount + ","
						+ (time[2] + time[3]) / iterCount);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static double[] measureQueryEfficiency(String indexPath, Connection con, List<ExperimentQuery> queries,
			String queryPrefix) throws SQLException {
		long startTime = System.currentTimeMillis();
		List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 1f);
		long middleTime = System.currentTimeMillis();
		long sum = 0;
		int idsCount = 0;
		for (QueryResult result : results) {
			List<String> ids = result.getTopDocuments().subList(0, Math.min(result.getTopDocuments().size(), 20))
					.stream().map(t -> t.id).collect(Collectors.toList());
			String query = String.format(queryPrefix, ids.toString().replace('[', '(').replace(']', ')'));
			idsCount += ids.size();
			long tmp = System.currentTimeMillis();
			submitSqlQuery(con, query);
			sum += System.currentTimeMillis() - tmp;
		}
		System.out.println(sum / results.size() + "\t" + idsCount);
		long endTime = System.currentTimeMillis();
		return new double[] { (middleTime - startTime) / queries.size(), (endTime - middleTime) / queries.size() };
	}

	static List<String> submitSqlQuery(Connection con, String query) throws SQLException {
		List<String> results = new ArrayList<String>();
		try (Statement stmt = con.createStatement()) {
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				String id = rs.getString("id");
				results.add(id);
			}
		} catch (SQLException e) {
			System.out.println(query);
			e.printStackTrace();
		}
		return results;
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
		System.out.println("Successfully connected to db");
		return conn;
	}

}
