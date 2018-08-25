package irstyle.api;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;

import irstyle.IRStyleQueryResult;
import irstyle.core.ExecPrepared;
import irstyle.core.Flags;
import irstyle.core.InitialMain;
import irstyle.core.Instance;
import irstyle.core.JDBCaccess;
import irstyle.core.MIndexAccess;
import irstyle.core.Relation;
import irstyle.core.Result;
import irstyle.core.Schema;
import query.ExperimentQuery;
import wiki13.WikiRelationalEfficiencyExperiment;

public class IRStyleKeywordSearch {

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
				+ "information_schema.tables WHERE table_name LIKE 'TS_%' and table_schema like '"
				+ jdbcacc.getDatabaseName() + "';";
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
				ExperimentQuery query = result.getQuery();
				fw.write(query.getId() + "," + query.getText().replaceAll(",", " ") + "," + result.rrank() + ","
						+ result.execTime + "\n");
				fw.flush();
			}
		}
	}

	public static void printResults(List<IRStyleQueryResult> queryResults, String filename) throws IOException {
		try (FileWriter fw = new FileWriter(filename)) {
			for (IRStyleQueryResult result : queryResults) {
				ExperimentQuery query = result.getQuery();
				fw.write(query.getId() + "," + query.getFreq() + "," + query.getText().replaceAll(",", " ") + ","
						+ result.rrank() + "," + result.p20() + "," + result.recall() + "," + result.execTime + "\n");
				fw.flush();
			}
		}
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

	public static IRStyleQueryResult executeIRStyleQuery(JDBCaccess jdbcacc, Schema sch, Vector<Relation> relations,
			ExperimentQuery query, Map<String, List<String>> relnameValues) throws SQLException {
		MIndexAccess MIndx = new MIndexAccess(relations);
		Vector<String> allkeyw = new Vector<String>();
		// escaping single quotes
		allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
		int exectime = 0;
		long start = System.currentTimeMillis();
		MIndx.createTupleSetsFast(sch, allkeyw, jdbcacc.conn, relnameValues);
		long tuplesetTime = System.currentTimeMillis() - start;
		exectime += tuplesetTime;
		if (Params.DEBUG)
			System.out.println(" Time to create tuple sets: " + (tuplesetTime) + " (ms)");
		start = System.currentTimeMillis();
		Vector<?> CNs = sch.getCNs(Params.maxCNsize, allkeyw, sch, MIndx);
		long cnTime = System.currentTimeMillis() - start;
		exectime += cnTime;
		if (Params.DEBUG)
			System.out.println(" Time to get CNs=" + (cnTime) + " (ms) \n\t #CNs: " + CNs.size());
		ArrayList<Result> results = new ArrayList<Result>();
		int time = methodC(Params.N, Params.allKeywInResults, relations, allkeyw, CNs, results, jdbcacc);
		exectime += time;
		if (Params.DEBUG)
			System.out.println(" Time to search joint tuplesets: " + time);
		dropTupleSets(jdbcacc, relations);
		IRStyleQueryResult result = new IRStyleQueryResult(query, exectime);
		result.addIRStyleResults(results);
		result.tuplesetTime = tuplesetTime;
		if (Params.DEBUG)
			System.out.println(" R-rank = " + result.rrank());
		return result;
	}

	public static int aggregateArticleTuplesetSize = 0;
	public static int counter = 0;

	public static List<String> executeLuceneQuery(IndexReader reader, String queryText, String TextField,
			String IdField) throws ParseException, IOException {
		IndexSearcher searcher = new IndexSearcher(reader);
		searcher.setSimilarity(new BM25Similarity());
		QueryParser qp = new QueryParser(TextField, new StandardAnalyzer());
		Query query = qp.parse(QueryParser.escape(queryText));
		ScoreDoc[] scoreDocHits = searcher.search(query, Params.MAX_TS_SIZE).scoreDocs;
		List<String> results = new ArrayList<String>();
		for (int j = 0; j < scoreDocHits.length; j++) {
			Document doc = reader.document(scoreDocHits[j].doc);
			String docId = doc.get(IdField);
			results.add("(" + docId + "," + scoreDocHits[j].score + ")");
			if (scoreDocHits[j].score < scoreDocHits[0].score * 0.5) {
//				break;
			}
		}
		System.out.println(
				"\t score range = " + scoreDocHits[0].score + " - " + scoreDocHits[scoreDocHits.length - 1].score);
		System.out.println("\t TS size = " + results.size());
		aggregateArticleTuplesetSize += results.size();
		counter++;
		return results;
	}
}
