package wiki13.maple;

import java.io.FileInputStream;
import java.io.IOException;
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
	// step #2
	Properties config = new Properties();
	try (FileInputStream in = new FileInputStream("config.properties")) {
	    config.load(in);
	    try (Connection con = getDatabaseConnection(config.get("username"),
		    config.get("password"), config.get("db-url"))) {
		long startTime = System.currentTimeMillis();
		List<QueryResult> results = WikiExperiment
			.runQueriesOnGlobalIndex(indexPath, queries, gamma);
		for (QueryResult result : results) {
		    List<String> ids = result.getTopDocuments().stream()
			    .map(t -> t.id).collect(Collectors.toList());
		    submitSqlQuery(con, ids);
		}
		long spentTime = (System.currentTimeMillis() - startTime)
			/ 1000;
		System.out.println("Total time: " + spentTime + " secs");
		System.out.println("Time per query: "
			+ spentTime / results.size() + " secs");
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}

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

    static Connection getDatabaseConnection(Object user, Object password,
	    Object dbUrl) throws SQLException {
	Properties connectionProps = new Properties();
	System.out.println(user + ", " + password + ", " + dbUrl);
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
