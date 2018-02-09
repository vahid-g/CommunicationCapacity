package wiki13.maple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperiment;

public class RelationalExperiment {

    public static void main(String[] args) {
	// 1: query the main index and retrieve wik-id of the returned tuples
	// 2: build a sql query to retrieve img and links of the returned
	// tuples,
	// submit sql and report the final timing

	// step #1
	int partition = 1;
	String indexPath = WikiMapleExperiment.DATA_PATH + "wiki_index/"
		+ partition;
	float gamma = 1f;
	List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
		WikiMapleExperiment.MSN_QUERY_FILE_PATH,
		WikiMapleExperiment.MSN_QREL_FILE_PATH);
	long startTime = System.currentTimeMillis();
	List<QueryResult> results = WikiExperiment
		.runQueriesOnGlobalIndex(indexPath, queries, gamma);

	// step #2
	try (Connection con = getDatabaseConnection()) {
	    for (QueryResult result : results) {
		List<String> ids = result.getTopDocuments().stream()
			.map(t -> t.id).collect(Collectors.toList());
		submitSqlQuery(con, ids);
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	long spentTime = (System.currentTimeMillis() - startTime) / 1000;
	System.out.println("Total time: " + spentTime + " secs");
	System.out.println(
		"Time per query: " + spentTime / results.size() + " secs");

    }

    public static void submitSqlQuery(Connection con, List<String> ids)
	    throws SQLException {
	Statement stmt = null;
	String query = "SELECT * FROM article a, img i, link l "
		+ "WHERE a.id = i.id and a.id = l.id AND a.id in "
		+ ids.toString().replace('[', '(').replace(']', ')') + ";";
	try {
	    stmt = con.createStatement();
	    ResultSet rs = stmt.executeQuery(query);
	    while (rs.next()) {
		String id = rs.getString("id");
		System.out.println(id);
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	} finally {
	    if (stmt != null) {
		stmt.close();
	    }
	}
    }

    static Connection getDatabaseConnection() throws SQLException {
	Properties connectionProps = new Properties();
	connectionProps.put("user", "khodadaa");
	connectionProps.put("password", "");
	Connection conn = null;
	try {
	    conn = DriverManager.getConnection(
		    "jdbc:mysql://engr-db.engr.oregonstate.edu:3307/khodadaa",
		    connectionProps);
	} catch (SQLException e) {
	    e.printStackTrace();
	}
	return conn;
    }

}
