package irstyle;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import irstyle.core.ExecPrepared;
import irstyle.core.Flags;
import irstyle.core.InitialMain;
import irstyle.core.Instance;
import irstyle.core.JDBCaccess;
import irstyle.core.Relation;
import irstyle.core.Result;
import query.ExperimentQuery;
import wiki13.WikiRelationalEfficiencyExperiment;

public class IRStyleKeywordSearch {

	public static Vector<Relation> createRelations(String articleTable, String imageTable, String linkTable,
			String articleImageTable, String articleLinkTable, Connection conn) throws SQLException {
		// Note that to be able to match qrels with answers, the main table should be
		// the first relation and
		// the first attrib should be its ID
		Vector<Relation> relations = new Vector<Relation>();
		Relation rel = new Relation(articleTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("title", true, "VARCHAR2(256)");
		rel.addAttribute("text", true, "VARCHAR2(32000)");
		// rel.addAttribute("popularity", false, "INTEGER");
		rel.addAttr4Rel("id", articleImageTable);
		rel.addAttr4Rel("id", articleLinkTable);
		rel.setSize(DatabaseHelper.tableSize(articleTable, conn));
		relations.addElement(rel);

		rel = new Relation(articleImageTable);
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttribute("image_id", false, "INTEGER");
		rel.addAttr4Rel("article_id", articleTable);
		rel.addAttr4Rel("image_id", imageTable);
		rel.setSize(DatabaseHelper.tableSize(articleImageTable, conn));
		relations.addElement(rel);

		rel = new Relation(imageTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("src", true, "VARCHAR(256)");
		rel.addAttr4Rel("id", articleImageTable);
		rel.setSize(DatabaseHelper.tableSize(imageTable, conn));
		relations.addElement(rel);

		rel = new Relation(articleLinkTable);
		rel.addAttribute("link_id", false, "INTEGER");
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttr4Rel("link_id", linkTable);
		rel.addAttr4Rel("article_id", articleTable);
		rel.setSize(DatabaseHelper.tableSize(articleLinkTable, conn));
		relations.addElement(rel);

		rel = new Relation(linkTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("url", true, "VARCHAR(255)");
		rel.addAttr4Rel("id", articleLinkTable);
		rel.setSize(DatabaseHelper.tableSize(linkTable, conn));
		relations.addElement(rel);

		return relations;
	}

	static int methodB(int N, boolean allKeywInResults, Vector<Relation> relations, Vector<String> allkeyw,
			Vector<?> CNs, ArrayList<Result> results, JDBCaccess jdbcacc) {
		// Method B: get top-K from each CN
		int exectime = 0;
		ExecPrepared execprepared = null;
		for (int i = 0; i < CNs.size(); i++) {
			System.out.println(" processing " + CNs.get(i));
			ArrayList<?> nfreeTSs2 = new ArrayList<Object>(1);
			if (Flags.DEBUG_INFO2)// Flags.DEBUG_INFO2)
			{
				Instance inst = ((Instance) CNs.elementAt(i));
				Vector<?> v = inst.getAllInstances();
				for (int j = 0; j < v.size(); j++) {
					System.out.print(((Instance) v.elementAt(j)).getRelationName() + " ");
					for (int k = 0; k < ((Instance) v.elementAt(j)).keywords.size(); k++)
						System.out.print((String) ((Instance) v.elementAt(j)).keywords.elementAt(k));
				}
				System.out.println("");
			}
			String sql = ((Instance) CNs.elementAt(i)).getSQLstatementParameterized(relations, allkeyw, nfreeTSs2);
			execprepared = new ExecPrepared();
			System.out.println(" sql: " + sql);
			long start = System.currentTimeMillis();
			exectime += execprepared.ExecuteParameterized(jdbcacc, sql, nfreeTSs2, new ArrayList<String>(allkeyw), N,
					((Instance) CNs.elementAt(i)).getsize() + 1, results, allKeywInResults);
			// +1 because different size semantics than DISCOVER
			System.out.println(" Time = " + (System.currentTimeMillis() - start) + "(ms)");
		}
		Collections.sort(results, new Result.ResultComparator());
		if (Flags.RESULTS__SHOW_OUTPUT) {
			System.out.println("final results, one CN at a time");
			InitialMain.printResults(results, N);
		}
		System.out.println("Exec one CN at a time: total exec time = " + (exectime / 1000)
				+ " (s) with allKeywInResults=" + allKeywInResults + " #results==" + results.size() + " \n");
		return exectime;
	}

	public static int methodC(int N, boolean allKeywInResults, Vector<Relation> relations, Vector<String> allkeyw,
			Vector<?> CNs, ArrayList<Result> results, JDBCaccess jdbcacc) {
		// Method C: parallel execution
		int exectime = 0;
		ArrayList[] nfreeTSs = new ArrayList[CNs.size()];
		String[] sqls = new String[CNs.size()];
		int[] CNsize = new int[CNs.size()];
		for (int i = 0; i < CNs.size(); i++) {
			CNsize[i] = ((Instance) CNs.elementAt(i)).getsize() + 1;
			nfreeTSs[i] = new ArrayList<String>();
			sqls[i] = ((Instance) CNs.elementAt(i)).getSQLstatementParameterized(relations, allkeyw, nfreeTSs[i]);
		}
		ExecPrepared execprepared2 = new ExecPrepared();
		exectime = execprepared2.ExecuteParallel(jdbcacc, sqls, nfreeTSs, new ArrayList<String>(allkeyw), N, CNsize,
				results, allKeywInResults);
		if (Params.DEBUG)
			System.out.println(" Proccess CNs in parallel time = " + exectime + " (ms) " + allKeywInResults
					+ " #results==" + results.size());
		return exectime;
	}

	public static void dropTupleSets(JDBCaccess jdbcacc, Vector<Relation> relations) {
		for (Relation rel : relations) {
			jdbcacc.dropTable("TS_" + rel.getName());
		}
	}

	public static void dropAllTuplesets(JDBCaccess jdbcacc) throws SQLException {
		String sql = "SELECT CONCAT( 'DROP TABLE ', GROUP_CONCAT(table_name) , ';' ) AS statement FROM "
				+ "information_schema.tables WHERE table_name LIKE 'TS_%';";
		try (Statement stmt = jdbcacc.conn.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				String dropQuery = rs.getString("statement");
				if (dropQuery != null && !dropQuery.equals("")) {
					stmt.executeUpdate(dropQuery);
				}
			}
		}

	}

	public static void printRrankResults(List<IRStyleQueryResult> queryResults, String filename) throws IOException {
		try (FileWriter fw = new FileWriter(filename)) {
			for (IRStyleQueryResult result : queryResults) {
				ExperimentQuery query = result.query;
				fw.write(query.getId() + "," + query.getText().replaceAll(",", " ") + "," + result.rrank() + ","
						+ result.execTime + "\n");
				fw.flush();
			}
		}
	}

	public static void printResults(List<IRStyleQueryResult> queryResults, String filename) throws IOException {
		try (FileWriter fw = new FileWriter(filename)) {
			for (IRStyleQueryResult result : queryResults) {
				ExperimentQuery query = result.query;
				fw.write(query.getId() + "," + query.getFreq() + "," + query.getText().replaceAll(",", " ") + ","
						+ result.rrank() + "," + result.p20() + "," + result.recall() + "," + result.execTime + "\n");
				fw.flush();
			}
		}
	}

	public static JDBCaccess jdbcAccess() throws IOException {
		return jdbcAccess("wikipedia");
	}

	public static JDBCaccess jdbcAccess(String Database_name) throws IOException {
		// JDBC input
		// Server = "localhost";
		String Server = "vm-maple.eecs.oregonstate.edu";
		String Port = "3306";
		// end input
		Properties config = new Properties();
		try (InputStream in = WikiRelationalEfficiencyExperiment.class
				.getResourceAsStream("/config/config.properties")) {
			config.load(in);
		}
		String Username = config.getProperty("username");
		String Password = config.getProperty("password");
		JDBCaccess jdbcacc = new JDBCaccess(Server, Port, Database_name, Username, Password);
		return jdbcacc;
	}

	public static Map<ExperimentQuery, Integer> buildQueryRelcountMap(Connection conn, List<ExperimentQuery> queryList)
			throws SQLException {
		Map<ExperimentQuery, Integer> map = new HashMap<ExperimentQuery, Integer>();
		for (ExperimentQuery query : queryList) {
			map.put(query, DatabaseHelper.relCounts(conn, query.getQrelScoreMap().keySet()));
		}
		return map;
	}

}
