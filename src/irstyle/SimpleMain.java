package irstyle;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import irstyle_core.ExecPrepared;
import irstyle_core.Flags;
import irstyle_core.IRStyleMain;
import irstyle_core.Instance;
import irstyle_core.JDBCaccess;
import irstyle_core.MIndexAccess;
import irstyle_core.Relation;
import irstyle_core.Result;
import irstyle_core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiFilesPaths;

public class SimpleMain {

	static FileOutputStream output;
	Random ran = new Random();

	static JDBCaccess jdbcacc;
	static String Server;
	static String Port;
	static String Database_name;
	static String Username;
	static String Password;

	public static void main(String[] args) throws IOException {

		// start input
		int maxCNsize = 4;
		int numExecutions = 1;
		int N = 200;
		boolean allKeywInResults = true;

		// JDBC input
		// Server = "localhost";
		Server = "vm-maple.eecs.oregonstate.edu";
		Database_name = "wikipedia";
		Username = "root";
		Password = "M@ple!";
		Port = "3306";

		// end input

		for (int exec = 0; exec < numExecutions; exec++) {

			Schema sch = new Schema(
					"5 tbl_article_09 tbl_article_image_09 tbl_image_09_tk tbl_article_link_09 tbl_link_09"
							+ " tbl_article_09 tbl_article_image_09" + " tbl_article_image_09 tbl_image_09_tk"
							+ " tbl_article_09 tbl_article_link_09" + " tbl_article_link_09 tbl_link_09");
			Vector<Relation> relations = createRelations();

			// access master index and create tuple sets
			MIndexAccess MIndx = new MIndexAccess(relations);
			jdbcacc = new JDBCaccess(Server, Port, Database_name, Username, Password);
			dropTupleSets();

			WikiFilesPaths paths = null;
			paths = WikiFilesPaths.getMaplePaths();
			List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
					paths.getMsnQrelFilePath());
			try (FileWriter fw = new FileWriter("result.csv")) {
				for (ExperimentQuery query : queries) {
					System.out.println("processing " + query.getText());

					Vector<String> allkeyw = new Vector<String>();
					//allkeyw.addAll(Arrays.asList(query.getText().split(" ")));
					allkeyw.add("jimmy");
					allkeyw.add("hoffa");
					long time3 = System.currentTimeMillis();
					MIndx.createTupleSets2(sch, allkeyw, jdbcacc.conn);
					long time4 = System.currentTimeMillis();

					System.out.println("time to create tuple sets=" + (time4 - time3) + "ms");
					time3 = System.currentTimeMillis();
					/** returns a vector of instances (tuple sets) */ // P1
					Vector CNs = sch.getCNs(maxCNsize, allkeyw, sch, MIndx);
					// also prune identical CNs with P2 in place of
					time4 = System.currentTimeMillis();
					// IRStyleMain.writetofile("#CNs=" + CNs.size() + " Time to get CNs=" + (time4 -
					// time3) + "\r\n");
					System.out.println("#CNs=" + CNs.size() + " Time to get CNs=" + (time4 - time3) + "\r\n");
					ArrayList results = new ArrayList(1);
					double timeOneCN = 0;
					double timeParallel = 0;
					// Method B: get top-K from each CN
					ExecPrepared execprepared = null;
					int exectime = 0;
					results = new ArrayList(1);
					// for (int i = 0; i < CNs.size(); i++) {
					// ArrayList nfreeTSs2 = new ArrayList(1);
					// if (Flags.DEBUG_INFO2)// Flags.DEBUG_INFO2)
					// {
					// Instance inst = ((Instance) CNs.elementAt(i));
					// Vector v = inst.getAllInstances();
					// for (int j = 0; j < v.size(); j++) {
					// System.out.print(((Instance) v.elementAt(j)).getRelationName() + " ");
					// for (int k = 0; k < ((Instance) v.elementAt(j)).keywords.size(); k++)
					// System.out.print((String) ((Instance) v.elementAt(j)).keywords.elementAt(k));
					// }
					// System.out.println("");
					// }
					// String sql = ((Instance)
					// CNs.elementAt(i)).getSQLstatementParameterized(relations, allkeyw,
					// nfreeTSs2);
					// execprepared = new ExecPrepared();
					// exectime += execprepared.ExecuteParameterized(jdbcacc, sql, nfreeTSs2, new
					// ArrayList(allkeyw),
					// N, ((Instance) CNs.elementAt(i)).getsize() + 1, results, allKeywInResults);
					// // +1
					// // because
					// // different size semantics than DISCOVER
					// }
					// Collections.sort(results, new Result.ResultComparator());
					// if (Flags.RESULTS__SHOW_OUTPUT) {
					// System.out.println("final results, one CN at a time");
					// IRStyleMain.printResults(results, N);
					// }
					// IRStyleMain.printResults(results, N);
					// System.out.println(" Exec one CN at a time: total exec time = " + exectime
					// + " with allKeywInResults=" + allKeywInResults + " #results==" +
					// results.size());
					timeOneCN += exectime;
					// Method C: parallel execution
					exectime = 0;
					
					ArrayList[] nfreeTSs = new ArrayList[CNs.size()];
					String[] sqls = new String[CNs.size()];
					int[] CNsize = new int[CNs.size()];
					for (int i = 0; i < CNs.size(); i++) {
						CNsize[i] = ((Instance) CNs.elementAt(i)).getsize() + 1;
						nfreeTSs[i] = new ArrayList(1);
						sqls[i] = ((Instance) CNs.elementAt(i)).getSQLstatementParameterized(relations, allkeyw,
								nfreeTSs[i]);
					}
					execprepared = new ExecPrepared();
					exectime = execprepared.ExecuteParallel(jdbcacc, sqls, nfreeTSs, new ArrayList(allkeyw), N, CNsize,
							results, allKeywInResults);
					System.out.println(" Exec CNs in parallel: total exec time = " + exectime + allKeywInResults
							+ " #results==" + results.size());

					timeParallel += exectime;
					dropTupleSets();
					String queryText = query.getText();
					if (queryText.contains(",")) {
						queryText = "\"" + queryText + "\"";
					}
					fw.write(query.getId() + "," + queryText + "," + mrr(results, query) + "," + timeParallel + "\n");
				}
			}
		}
	}

	static double mrr(List<Result> results, ExperimentQuery query) {
		for (int i = 0; i < results.size(); i++) {
			String resultText = results.get(i).getStr();
			String resultId = resultText.substring(0, resultText.indexOf(" - "));
			if (query.getQrelScoreMap().keySet().contains(resultId)) {
				return 1.0 / (i + 1);
			}
		}
		return 0;
	}

	static Vector<Relation> createRelations() { // schema 1
		// Note that to be able to match qrels with answers, the main table should be
		// the first relation and
		// the first attrib should be its ID
		Vector<Relation> relations = new Vector<Relation>();

		Relation rel = new Relation("tbl_article_09");
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
		rel.addAttr4Rel("article_id", "tbl_article_09");
		rel.addAttr4Rel("image_id", "tbl_image_09_tk");
		rel.setSize(3840433);
		relations.addElement(rel);

		rel = new Relation("tbl_image_09_tk");
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("src", true, "VARCHAR(256)");
		rel.addAttr4Rel("id", "tbl_article_image_09");
		rel.setSize(1183070);
		relations.addElement(rel);

		rel = new Relation("tbl_article_link_09");
		rel.addAttribute("link_id", false, "INTEGER");
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttr4Rel("link_id", "tbl_link_09");
		rel.addAttr4Rel("article_id", "tbl_article_09");
		rel.setSize(120916125);
		relations.addElement(rel);

		rel = new Relation("tbl_link_09");
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("url", true, "VARCHAR(255)");
		rel.addAttr4Rel("id", "tbl_article_link_09");
		rel.setSize(9766351);
		relations.addElement(rel);

		return relations;
	}

	static void dropTupleSets() {
		jdbcacc.dropTable("TS_tbl_article_09");
		jdbcacc.dropTable("TS_tbl_article_image_09");
		jdbcacc.dropTable("TS_tbl_image_09_tk");
		jdbcacc.dropTable("TS_tbl_article_link_09");
		jdbcacc.dropTable("TS_tbl_link_09");

	}

}
