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
	// step #1: query the main index and retrieve wik-id of the returned
	// tuples
	int partition = Integer.parseInt(args[0]);
	String indexPath = WikiMapleExperiment.DATA_PATH + "wiki_index/"
		+ partition;
	float gamma = 1f;
	List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
		WikiMapleExperiment.MSN_QUERY_FILE_PATH,
		WikiMapleExperiment.MSN_QREL_FILE_PATH);

	// step #2: build a sql query to retrieve img and links of the returned
	// tuples, submit sql and report the final timing
	Properties config = new Properties();
	try (InputStream in = RelationalExperiment.class
		.getResourceAsStream("/config/config.properties")) {
	    config.load(in);
	    try (Connection con = getDatabaseConnection(config.get("username"),
		    config.get("password"), config.get("db-url"))) {
		long startTime = System.currentTimeMillis();
		List<QueryResult> results = WikiExperiment
			.runQueriesOnGlobalIndex(indexPath, queries, gamma);
		for (QueryResult result : results) {
		    List<String> ids = result.getTopDocuments()
			    .subList(0,
				    Math.min(result.getTopDocuments().size(),
					    20))
			    .stream().map(t -> t.id)
			    .collect(Collectors.toList());
		    submitSqlQuery(con, ids);
		}
		long spentTime = (System.currentTimeMillis() - startTime)
			/ 1000;
		System.out.println("Total time: " + spentTime + " secs");
		System.out.println("Time per query: "
			+ spentTime / (double)results.size() + " secs");
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public static List<String> submitSqlQuery(Connection con, List<String> ids)
	    throws SQLException {
	Statement stmt = null;
	List<String> results = new ArrayList<String>();
	String query = "SELECT a.id FROM tbl_article_wiki13 a left join "
		+ "tbl_article_image_09 i on a.id = i.article_id left join "
		+ "tbl_article_link_09 l on id=l.article_id WHERE a.id in "
		+ ids.toString().replace('[', '(').replace(']', ')') + ";";
	System.out.println(query);
	try {
	    stmt = con.createStatement();
	    ResultSet rs = stmt.executeQuery(query);
	    System.out.println(rs.getFetchSize());
	    while (rs.next()) {
		String id = rs.getString("id");
		results.add(id);
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	} finally {
	    if (stmt != null) {
		stmt.close();
	    }
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
