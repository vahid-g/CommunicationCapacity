package irstyle;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import irstyle_core.ExecPrepared;
import irstyle_core.Flags;
import irstyle_core.InitialMain;
import irstyle_core.Instance;
import irstyle_core.JDBCaccess;
import irstyle_core.MIndexAccess;
import irstyle_core.Relation;
import irstyle_core.Result;
import irstyle_core.Schema;
import query.ExperimentQuery;
import query.Qrel;
import query.QueryServices;
import wiki13.WikiFilesPaths;
import wiki13.WikiRelationalEfficiencyExperiment;

public class IRStyleMain {

	static int maxCNsize = 5;
	static int numExecutions = 1;
	static int N = 100;
	static boolean allKeywInResults = false;

	public static void main(String[] args) throws IOException {
		JDBCaccess jdbcacc = jdbcAccess();
		for (int exec = 0; exec < numExecutions; exec++) {
			String articleTable = "tbl_article_09";
			String imageTable = "tbl_image_09_tk";
			String linkTable = "tbl_link_09";
			String articleImageTable = "tbl_article_image_09";
			String articleLinkTable = "tbl_article_link_09";
			String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
					+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
					+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
					+ articleLinkTable + " " + linkTable;
			Vector<Relation> relations = createRelations(articleTable, imageTable, linkTable);
			dropTupleSets(jdbcacc, relations);
			WikiFilesPaths paths = null;
			paths = WikiFilesPaths.getMaplePaths();
			List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
					paths.getMsnQrelFilePath());
			Collections.shuffle(queries, new Random(1));
			queries = queries.subList(0, 50);
			List<QueryResult> queryResults = new ArrayList<QueryResult>();
//			queries = new ArrayList<ExperimentQuery>();
//			ExperimentQuery eq = new ExperimentQuery(1, "Nero", 1);
//			eq.addRelevantAnswer(new Qrel(1, "21632", 1));
//			queries.add(eq);
			int loop = 1;
			for (ExperimentQuery query : queries) {
				System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
				Schema sch = new Schema(schemaDescription);
				QueryResult result = executeIRStyleQuery(jdbcacc, sch, relations, query);
				queryResults.add(result);
			}
			printResults(queryResults, "ir_result.csv");
		}
	}

	static JDBCaccess jdbcAccess() throws IOException {
		// JDBC input
		// Server = "localhost";
		String Server = "vm-maple.eecs.oregonstate.edu";
		String Database_name = "wikipedia";
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

	static Vector<Relation> createRelations(String articleTable, String imageTable, String linkTable) {
		// Note that to be able to match qrels with answers, the main table should be
		// the first relation and
		// the first attrib should be its ID
		Vector<Relation> relations = new Vector<Relation>();
		Relation rel = new Relation(articleTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("title", true, "VARCHAR2(256)");
		rel.addAttribute("text", true, "VARCHAR2(32000)");
		// rel.addAttribute("popularity", false, "INTEGER");
		rel.addAttr4Rel("id", "tbl_article_image_09");
		rel.addAttr4Rel("id", "tbl_article_link_09");
		rel.setSize(233909);
		relations.addElement(rel);

		rel = new Relation("tbl_article_image_09");
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttribute("image_id", false, "INTEGER");
		rel.addAttr4Rel("article_id", articleTable);
		rel.addAttr4Rel("image_id", imageTable);
		rel.setSize(3840433);
		relations.addElement(rel);

		rel = new Relation(imageTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("src", true, "VARCHAR(256)");
		rel.addAttr4Rel("id", "tbl_article_image_09");
		rel.setSize(1183070);
		relations.addElement(rel);

		rel = new Relation("tbl_article_link_09");
		rel.addAttribute("link_id", false, "INTEGER");
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttr4Rel("link_id", linkTable);
		rel.addAttr4Rel("article_id", articleTable);
		rel.setSize(120916125);
		relations.addElement(rel);

		rel = new Relation(linkTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("url", true, "VARCHAR(255)");
		rel.addAttr4Rel("id", "tbl_article_link_09");
		rel.setSize(9766351);
		relations.addElement(rel);

		return relations;
	}

	static QueryResult executeIRStyleQuery(JDBCaccess jdbcacc, Schema sch, Vector<Relation> relations,
			ExperimentQuery query) {
		MIndexAccess MIndx = new MIndexAccess(relations);
		Vector<String> allkeyw = new Vector<String>();
		// escaping single quotes
		allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
		int exectime = 0;
		long time3 = System.currentTimeMillis();
		MIndx.createTupleSets2(sch, allkeyw, jdbcacc.conn);
		long time4 = System.currentTimeMillis();
		exectime += time4 - time3;
		System.out.println(" Time to create tuple sets: " + (time4 - time3) + " (ms)");
		time3 = System.currentTimeMillis();
		/** returns a vector of instances (tuple sets) */ // P1
		Vector<?> CNs = sch.getCNs(maxCNsize, allkeyw, sch, MIndx);
		for (Object v : CNs) {
			System.out.println(v);
		}
		time4 = System.currentTimeMillis();
		exectime += time4 - time3;
		System.out.println(" #CNs=" + CNs.size() + " Time to get CNs=" + (time4 - time3) + " (ms)");
		ArrayList<Result> results = new ArrayList<Result>(1);
		exectime += methodC(N, allKeywInResults, relations, allkeyw, CNs, results, jdbcacc);
		dropTupleSets(jdbcacc, relations);
		double rrank = rrank(results, query);
		System.out.println(" R-rank = " + rrank);
		QueryResult result = new QueryResult(query, rrank, exectime);
		return result;
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

	static int methodC(int N, boolean allKeywInResults, Vector<Relation> relations, Vector<String> allkeyw,
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
		System.out.println(" Exec CNs in parallel: total exec time = " + exectime + " (ms) " + allKeywInResults
				+ " #results==" + results.size());
		return exectime;
	}

	static double rrank(List<Result> results, ExperimentQuery query) {
		for (int i = 0; i < results.size(); i++) {
			String resultText = results.get(i).getStr();
			String resultId = resultText.substring(0, resultText.indexOf(" - "));
			if (query.getQrelScoreMap().keySet().contains(resultId)) {
				return 1.0 / (i + 1);
			}
		}
		return 0;
	}

	static void dropTupleSets(JDBCaccess jdbcacc, Vector<Relation> relations) {
		for (Relation rel : relations) {
			jdbcacc.dropTable("TS_" + rel.getName());
		}
	}

	static void printResults(List<QueryResult> queryResults, String filename) throws IOException {
		try (FileWriter fw = new FileWriter(filename)) {
			for (QueryResult result : queryResults) {
				ExperimentQuery query = result.query;
				fw.write(query.getId() + "," + query.getText().replaceAll(",", " ") + "," + result.rrank + ","
						+ result.execTime + "\n");
				fw.flush();
			}
		}
	}

}
