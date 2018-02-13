package wiki13.maple;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperiment;

public class RelationalExperiment {

    public static void main(String[] args) {
	// int docsInSubset = 1163610;
	String subsetIndexPath = WikiMapleExperiment.DATA_PATH
		+ "wiki_index/99";
	String indexPath = WikiMapleExperiment.DATA_PATH + "wiki_index/100";
	Properties config = new Properties();
	try (InputStream in = RelationalExperiment.class
		.getResourceAsStream("/config/config.properties")) {
	    config.load(in);
	    try (Connection con = getDatabaseConnection(config.get("username"),
		    config.get("password"), config.get("db-url"))) {
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
			WikiMapleExperiment.MSN_QUERY_FILE_PATH,
			WikiMapleExperiment.MSN_QREL_FILE_PATH);
		queries = queries.subList(0, 200);
		String prefix = "SELECT a.id FROM tmp_article_1 a left join "
			+ "tmp_article_image_1 i on a.id = i.article_id left join "
			+ "tmp_article_link_1 l on a.id=l.article_id WHERE a.id in ";
		measureQueryEfficiency(subsetIndexPath, con, queries, prefix);
		prefix = "SELECT a.id FROM tbl_article_wiki13 a left join "
			+ "tbl_article_image_09 i on a.id = i.article_id left join "
			+ "tbl_article_link_09 l on a.id=l.article_id WHERE a.id in ";
		measureQueryEfficiency(indexPath, con, queries, prefix);
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    private static void measureQueryEfficiency(String indexPath, Connection con,
	    List<ExperimentQuery> queries, String queryPrefix)
	    throws SQLException {
	long startTime = System.currentTimeMillis();
	List<QueryResult> results = WikiExperiment
		.runQueriesOnGlobalIndex(indexPath, queries, 1f);
	int zeroResultCounter = 0;
	for (QueryResult result : results) {
	    List<String> ids = result.getTopDocuments()
		    .subList(0, Math.min(result.getTopDocuments().size(), 20))
		    .stream().map(t -> t.id).collect(Collectors.toList());
	    if (ids.size() > 0) {
		String query = queryPrefix
			+ ids.toString().replace('[', '(').replace(']', ')')
			+ ";";
		submitSqlQuery(con, query);
	    } else {
		zeroResultCounter++;
	    }
	}
	long spentTime = (System.currentTimeMillis() - startTime) / 1000;
	System.out.println("Total time: " + spentTime + " secs");
	System.out.println("Time per query: "
		+ spentTime / (double) results.size() + " secs");
	System.out.println("Zero result counter: " + zeroResultCounter);
    }

    public static List<String> submitSqlQuery(Connection con, String query)
	    throws SQLException {
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

    static Connection getDatabaseConnection(Object user, Object password,
	    Object dbUrl) throws SQLException {
	Properties connectionProps = new Properties();
	connectionProps.put("user", user);
	connectionProps.put("password", password);
	Connection conn = null;
	try {
	    conn = DriverManager.getConnection(dbUrl.toString(),
		    connectionProps);
	} catch (SQLException e) {
	    e.printStackTrace();
	    return null;
	}
	System.out.println("Successfully connected to db");
	return conn;
    }

}
