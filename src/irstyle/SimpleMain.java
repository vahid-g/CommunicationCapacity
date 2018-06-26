package irstyle;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import irstyle_core.ExecPrepared;
import irstyle_core.Flags;
import irstyle_core.Instance;
import irstyle_core.JDBCaccess;
import irstyle_core.MIndexAccess;
import irstyle_core.Relation;
import irstyle_core.Result;
import irstyle_core.Schema;
import irstyle_core.IRStyleMain;
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

	public static void main(String[] args) {

		// start input
		int maxCNsize = 4;
		int numExecutions = 1;
		int N = 5;
		boolean allKeywInResults = true;

		// JDBC input
		// Server = "localhost";
		Server = "vm-maple.eecs.oregonstate.edu";
		Database_name = "wikipedia";
		Username = "root";
		Password = "M@ple!";
		Port = "3306";

		// end input

		Vector<String> inputkeywords = new Vector<String>();
		for (int exec = 0; exec < numExecutions; exec++) {

			Schema sch = new Schema("2 mem_article_1 mem_article_link_1 " + "mem_article_1 mem_article_link_1");
			Vector<Relation> relations = IRStyleMain.createRelations();

			// access master index and create tuple sets
			MIndexAccess MIndx = new MIndexAccess(relations);
			jdbcacc = new JDBCaccess(Server, Port, Database_name, Username, Password);
			IRStyleMain.dropTupleSets();

			WikiFilesPaths paths = null;
			paths = WikiFilesPaths.getMaplePaths();
			List<ExperimentQuery> queries;
			queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(), paths.getMsnQrelFilePath());
			for (ExperimentQuery query : queries) {
				System.out.println("processing " + query.getText());

				Vector<String> allkeyw = new Vector<String>();
				allkeyw.addAll(Arrays.asList(query.getText().split(" ")));
				int NumKeyw = allkeyw.size();
				long time3 = System.currentTimeMillis();
				MIndx.createTupleSets2(sch, allkeyw, jdbcacc.conn);
				long time4 = System.currentTimeMillis();

				// System.out.println("time to create tuple sets="+(time4-time3));
				// get CN's
				time3 = System.currentTimeMillis();
				/** returns a vector of instances (tuple sets) */ // P1
				Vector CNs = sch.getCNs(maxCNsize, allkeyw, sch, MIndx); // also prune identical CNs with P2 in place of

				time4 = System.currentTimeMillis();
				IRStyleMain.writetofile("#CNs=" + CNs.size() + " Time to get CNs=" + (time4 - time3) + "\r\n");

				ArrayList results = new ArrayList(1);
				double timeOneCN = 0;
				double timeParallel = 0;
				// Method B: get top-K from each CN
				ExecPrepared execprepared = null;
				int exectime = 0;
				results = new ArrayList(1);
				for (int i = 0; i < CNs.size(); i++) {
					ArrayList nfreeTSs2 = new ArrayList(1);
					if (Flags.DEBUG_INFO2)// Flags.DEBUG_INFO2)
					{
						Instance inst = ((Instance) CNs.elementAt(i));
						Vector v = inst.getAllInstances();
						for (int j = 0; j < v.size(); j++) {
							System.out.print(((Instance) v.elementAt(j)).getRelationName() + " ");
							for (int k = 0; k < ((Instance) v.elementAt(j)).keywords.size(); k++)
								System.out.print((String) ((Instance) v.elementAt(j)).keywords.elementAt(k));
						}
						System.out.println("");
					}
					String sql = ((Instance) CNs.elementAt(i)).getSQLstatementParameterized(relations, allkeyw,
							nfreeTSs2);
					execprepared = new ExecPrepared();
					exectime += execprepared.ExecuteParameterized(jdbcacc, sql, nfreeTSs2, new ArrayList(allkeyw), N,
							((Instance) CNs.elementAt(i)).getsize() + 1, results, allKeywInResults); // +1 because
					// different size semantics than DISCOVER
				}
				Collections.sort(results, new Result.ResultComparator());
				if (Flags.RESULTS__SHOW_OUTPUT) {
					System.out.println("final results, one CN at a time");
					IRStyleMain.printResults(results, N);
				}
				IRStyleMain.printResults(results, N);
				System.out.println(" Exec one CN at a time: total exec time = " + exectime + " with allKeywInResults="
						+ allKeywInResults + " #results==" + results.size());
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
						allKeywInResults);

				System.out.println(" Exec CNs in parallel: total exec time = " + exectime + allKeywInResults
						+ " #results==" + results.size());
				timeParallel += exectime;

				// dropTupleSets();
				break;
			}
		}

	}

}
