package irstyle_core;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Vector;

public class InitialMain {

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
		int NumKeyw = 2;
		int numExecutions = 1;
		int N = 5;
		boolean allKeywInResults = true;
		// Schema 1
		// Schema sch=new Schema("9 C CY Y YP P1 P2 PP PA A "
		// +"C CY Y CY Y YP P1 YP P2 YP P1 PP P2 PP P1 PA P2 PA A PA");

		int experiment = 3;
		// 2:exec times, 3:create text indices String, 9:nothing

		// JDBC input
		// Server = "TERIYAKI.ucsd.edu";
		Server = "localhost";
		Database_name = "wikipedia";
		Username = "";
		Password = "";
		Port = "3306";

		// end input

		Vector inputkeywords = new Vector(1);
		boolean keywordsAreInput = false;
		if ((args != null) && (args.length == 1)) {
			System.out.println("args: maxCNsize NumKeyw numExecutions N allKeywInResults [optional_keywords]");
			System.exit(1);
		} else if ((args != null) && (args.length == 5)) {
			int argindex = 0;
			maxCNsize = Integer.parseInt(args[argindex++]);
			NumKeyw = Integer.parseInt(args[argindex++]);
			numExecutions = Integer.parseInt(args[argindex++]);
			N = Integer.parseInt(args[argindex++]);
			allKeywInResults = Boolean.valueOf(args[argindex++]).booleanValue();
		} else if ((args != null) && (args.length > 5)) {
			int argindex = 0;
			maxCNsize = Integer.parseInt(args[argindex++]);
			NumKeyw = Integer.parseInt(args[argindex++]);
			numExecutions = Integer.parseInt(args[argindex++]);
			N = Integer.parseInt(args[argindex++]);
			allKeywInResults = Boolean.valueOf(args[argindex++]).booleanValue();
			for (int i = 0; i < NumKeyw; i++)
				inputkeywords.add(args[argindex++]);
			keywordsAreInput = true;
		}
		if (experiment == 3) {
			Vector relations = createRelations();
			String str = (new MasterIndex()).createInterMediaScript(relations);
			System.out.println(str);
		}
		if (experiment == 2) {
			double timeOracle = 0, timeOneCN = 0, timeParallel = 0, timeOneCNsymmetric = 0, timeParallelsymmetric = 0;
			for (int exec = 0; exec < numExecutions; exec++) {
				Vector allkeyw = new Vector(2);
				// Schema sch=new Schema("8 C CY Y YP P1 PP PA A "
				// +"C CY CY YP Y CY Y YP P1 YP P1 PP YP PP YP PA PP PA A PA");
				// Schema sch=new Schema("8 C CY Y YP P1 PP PA A "
				// +"C CY CY YP Y CY Y YP P1 YP P1 PP YP PP YP PA PP PA A PA PP P1"); //added
				// PP->P, so P->PP->P is possible
				Schema sch = new Schema(
						// "9 C CY Y YP P1 P2 PP PA A " + "C CY Y CY Y YP P1 YP P1 PP P1 PA A PA P2 YP
						// P2 PP P2 PA");
						"2 mem_article_1 mem_article_link_1 " + "mem_article_1 mem_article_link_1");
				Vector relations = createRelations();
				if (!keywordsAreInput)
					for (int j = 0; j < NumKeyw; j++)
						allkeyw.add(keywords.getRandomKeyword());
				else
					allkeyw = inputkeywords;

				allkeyw.clear();
				allkeyw.add("Afghanistan");
				// allkeyw.add("Widom");
				NumKeyw = allkeyw.size();

				String st = "";
				for (int j = 0; j < NumKeyw; j++)
					st += (String) allkeyw.get(j) + " ";
				System.out.println("keywords: " + st);
				// access master index and create tuple sets
				MIndexAccess MIndx = new MIndexAccess(relations);
				jdbcacc = new JDBCaccess(Server, Port, Database_name, Username, Password);
				dropTupleSets();
				long time3 = System.currentTimeMillis();
				MIndx.createTupleSets2(sch, allkeyw, jdbcacc.conn);
				long time4 = System.currentTimeMillis();
				// System.out.println("time to create tuple sets="+(time4-time3));
				// get CN's
				time3 = System.currentTimeMillis();
				/** returns a vector of instances (tuple sets) */ // P1
				Vector CNs = sch.getCNs(maxCNsize, allkeyw, sch, MIndx); // also prune identical CNs with P2 in place of

				time4 = System.currentTimeMillis();
				writetofile("#CNs=" + CNs.size() + " Time to get CNs=" + (time4 - time3) + "\r\n");

				// Method A: send SQL to Oracle
				time3 = System.currentTimeMillis();
				ArrayList results = new ArrayList(1);
				String sqlStatements = "";
				for (int i = 0; i < CNs.size(); i++) {
					String sql = ((Instance) CNs.elementAt(i)).getSQLstatementOrderedwScores(relations, allkeyw);
					if (Flags.DEBUG_INFO)
						System.out.print(sql);
					sqlStatements += sql + " ; ";
					if (!allKeywInResults)
						jdbcacc.getTopNResults(sql, N, results);
					else
						jdbcacc.getTopNResultsAllKeyw(sql, N, results, new ArrayList(allkeyw));
				}
				Collections.sort(results, new Result.ResultComparator());
				if (Flags.RESULTS__SHOW_OUTPUT) {
					System.out.println("final results , by sending queries to Oracle");
					printResults(results, N);
				}
				time4 = System.currentTimeMillis();
				System.out.println("SQL statements: " + sqlStatements);
				System.out.println("Time to get top-" + N + " results from SQL statements, = " + (time4 - time3)
						+ "(ms) with allKeywInResults=" + allKeywInResults + " #results==" + results.size());
				timeOracle += (time4 - time3);

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
					printResults(results, N);
				}
				printResults(results, N);
				System.out.println(" Exec one CN at a time: total exec time = " + exectime + " with allKeywInResults="
						+ allKeywInResults + " #results==" + results.size());
				timeOneCN += exectime;
				// Method C: parallel execution
				exectime = 0;
				results = new ArrayList(1);
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
						results,allKeywInResults);

				System.out.println(" Exec CNs in parallel: total exec time = " + exectime + allKeywInResults
						+ " #results==" + results.size());
				timeParallel += exectime;

				// Method D: get top-K from each CN going down on TSs symmetrically

				exectime = 0;
				results = new ArrayList(1);
				for (int i = 0; i < CNs.size(); i++) {
					ArrayList nfreeTSs2 = new ArrayList(1);
					if (false)// Flags.DEBUG_INFO2)
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
					// exectime+=execprepared.ExecuteParameterizedSymmetric(jdbcacc, sql, nfreeTSs2,
					// new ArrayList( allkeyw), N, ((Instance)
					// CNs.elementAt(i)).getsize()+1,results,allKeywInResults); //+1 because
					// different size semantics than DISCOVER
				}
				Collections.sort(results, new Result.ResultComparator());
				if (Flags.RESULTS__SHOW_OUTPUT) {
					System.out.println("final results, one CN at a time");
					printResults(results, N);
				}
				System.out.println(" Exec one CN at a time symmetric: total exec time = " + exectime
						+ " with allKeywInResults=" + allKeywInResults + " #results==" + results.size());
				timeOneCNsymmetric += exectime;
				// Method E: parallel execution symmetric going down
				exectime = 0;
				nfreeTSs = new ArrayList[CNs.size()];
				sqls = new String[CNs.size()];
				CNsize = new int[CNs.size()];
				for (int i = 0; i < CNs.size(); i++) {
					CNsize[i] = ((Instance) CNs.elementAt(i)).getsize() + 1;
					nfreeTSs[i] = new ArrayList(1);
					sqls[i] = ((Instance) CNs.elementAt(i)).getSQLstatementParameterized(relations, allkeyw,
							nfreeTSs[i]);
				}
				execprepared = new ExecPrepared();
				exectime = execprepared.ExecuteParallelSymmetric(jdbcacc, sqls, nfreeTSs, new ArrayList(allkeyw), N,
						CNsize, allKeywInResults);

				System.out.println(" Exec CNs in parallel symmetric: total exec time = " + exectime
						+ ", allKeywInResults=" + allKeywInResults + " #results==" + results.size());
				timeParallelsymmetric += exectime;

				// dropTupleSets();
			}
			System.out.println("Summary: #keywords=" + NumKeyw + " N= " + N + " numexec = " + numExecutions
					+ " maxCNsize = " + maxCNsize + " avg time Oracle = " + timeOracle / numExecutions
					+ " avg time OneCN at a time = " + timeOneCN / numExecutions
					+ " avg time OneCN at a time symmetric= " + timeOneCNsymmetric / numExecutions
					+ " avg time parallel = " + timeParallel / numExecutions + " avg time parallel symmetric = "
					+ timeParallelsymmetric / numExecutions);
		}
	}

	public static void execSQLVector(Vector Commands) {
		if (Commands != null)
			for (int j = 0; j < Commands.size(); j++) {
				String command = (String) Commands.elementAt(j);
				jdbcacc.execute(command);
			}
	}
	/*
	 * static Vector createRelations() { //schema 1 Vector relations=new Vector(1);
	 * 
	 * Relation rel=new Relation("C"); rel.addAttribute("ID",false,"INTEGER");
	 * rel.addAttribute("TEXT",true,"VARCHAR2(99)"); rel.addAttr4Rel("ID","CY");
	 * rel.setSize(200000); relations.addElement(rel);
	 * 
	 * rel=new Relation("CY"); rel.addAttribute("CY_C",false,"INTEGER");
	 * rel.addAttribute("CY_Y",false,"INTEGER"); rel.addAttr4Rel("CY_C","C");
	 * rel.addAttr4Rel("CY_Y","Y"); rel.setSize(800000); relations.addElement(rel);
	 * 
	 * rel=new Relation("Y"); rel.addAttribute("ID",false,"INTEGER");
	 * rel.addAttribute("TEXT",true,"VARCHAR2(99)"); rel.addAttr4Rel("ID","CY");
	 * rel.addAttr4Rel("ID","YP"); rel.setSize(200000); relations.addElement(rel);
	 * 
	 * rel=new Relation("YP"); rel.addAttribute("YP_Y",false,"INTEGER");
	 * rel.addAttribute("YP_P",false,"INTEGER"); rel.addAttr4Rel("YP_Y","Y");
	 * rel.addAttr4Rel("YP_P","P1"); rel.addAttr4Rel("YP_P","P2");
	 * rel.setSize(800000); relations.addElement(rel);
	 * 
	 * rel=new Relation("P1"); rel.addAttribute("ID",false,"INTEGER");
	 * rel.addAttribute("TEXT",true,"VARCHAR2(99)"); rel.addAttr4Rel("ID","YP");
	 * rel.addAttr4Rel("ID","PP"); rel.addAttr4Rel("ID","PA"); rel.setSize(200000);
	 * relations.addElement(rel);
	 * 
	 * rel=new Relation("P2"); rel.addAttribute("ID",false,"INTEGER");
	 * rel.addAttribute("TEXT",true,"VARCHAR2(99)"); rel.addAttr4Rel("ID","YP");
	 * rel.addAttr4Rel("ID","PP"); rel.addAttr4Rel("ID","PA"); rel.setSize(200000);
	 * relations.addElement(rel);
	 * 
	 * rel=new Relation("PP"); rel.addAttribute("PP_P1",false,"INTEGER");
	 * rel.addAttribute("PP_P2",false,"INTEGER"); rel.addAttr4Rel("PP_P1","P1");
	 * rel.addAttr4Rel("PP_P2","P2"); rel.setSize(800000);
	 * relations.addElement(rel);
	 * 
	 * rel=new Relation("PA"); rel.addAttribute("PA_P",false,"INTEGER");
	 * rel.addAttribute("PA_A",false,"INTEGER"); rel.addAttr4Rel("PA_P","P1");
	 * rel.addAttr4Rel("PA_P","P2"); rel.addAttr4Rel("PA_A","A");
	 * rel.setSize(800000); relations.addElement(rel);
	 * 
	 * rel=new Relation("A"); rel.addAttribute("ID",false,"INTEGER");
	 * rel.addAttribute("TEXT",true,"VARCHAR2(99)"); rel.addAttr4Rel("ID","PA");
	 * rel.setSize(200000); relations.addElement(rel);
	 * 
	 * return relations; }
	 */

	static Vector createRelations() { // schema 1
		Vector relations = new Vector(1);

		Relation rel = new Relation("mem_article_1");
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("title", true, "VARCHAR2(256)");
		rel.addAttribute("text", true, "VARCHAR2(32000)");
		// rel.addAttribute("popularity", false, "INTEGER");
		rel.addAttr4Rel("id", "mem_article_link_1"); 
		rel.setSize(200000);
		relations.addElement(rel);

		rel = new Relation("mem_article_link_1");
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttribute("link_id", false, "INTEGER");
		rel.addAttr4Rel("article_id", "mem_article_1");
		//rel.addAttr4Rel("link_id", "mem_article_1");
		rel.setSize(200000);
		relations.addElement(rel);
		return relations;
	}

	public static void writetofile(String str) {
		// try
		// {
		// output.write((str+"\n").getBytes() );
		System.out.println(str);
		// }
		// catch(IOException e1)
		// {
		// System.out.println("exception class: " + e1.getClass() +" with message: " +
		// e1.getMessage());
		// }
	}

	public static void writeVectortofile(Vector strV) {// strV is a String Vector
		try {
			String str = "";
			for (int i = 0; i < strV.size(); i++)
				str += (String) strV.elementAt(i) + ";\r\n";
			output.write((str + "\r\n").getBytes());
		} catch (IOException e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage());
		}
	}

	private static void dropTupleSets() {
		jdbcacc.dropTable("TS_mem_article_1");
		jdbcacc.dropTable("TS_mem_article_link_1"); // TODO

	}

	public static void printResults(ArrayList results, int N) {
		for (int i = 0; i < results.size(); i++) {
			if (i >= N)
				break;
			((Result) results.get(i)).print();
			;
		}
	}

}
