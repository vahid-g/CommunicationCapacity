package irstyle;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;

import irstyle_core.ExecPrepared;
import irstyle_core.Instance;
import irstyle_core.JDBCaccess;
import irstyle_core.MIndexAccess;
import irstyle_core.Relation;
import irstyle_core.Result;
import irstyle_core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiFilesPaths;
import wiki13.WikiRelationalEfficiencyExperiment;

public class IRStyleLuceneMain {

	static int maxCNsize = 5;
	static int numExecutions = 1;
	static int N = 100;
	static boolean allKeywInResults = false;

	static class QueryResult {
		ExperimentQuery query;
		double rrank = 0;
		long execTime = 0;

		public QueryResult(ExperimentQuery query, double rrank, long execTime) {
			this.query = query;
			this.rrank = rrank;
			this.execTime = execTime;
		}
	}

	public static void main(String[] args) throws Exception {
		JDBCaccess jdbcacc = jdbcAccess();
		for (int exec = 0; exec < numExecutions; exec++) {
			String articleTable = "tbl_article_wiki13";
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
			queries = new ArrayList<ExperimentQuery>();
			queries.add(new ExperimentQuery(1, "Nero", 1));
			String baseDir = "/data/ghadakcv/wikipedia/";
			try (IndexReader articleReader = DirectoryReader
					.open(FSDirectory.open(Paths.get(baseDir + "tbl_article_09/100")));
					IndexReader imageReader = DirectoryReader
							.open(FSDirectory.open(Paths.get(baseDir + "tbl_image_pop/100")));
					IndexReader linkReader = DirectoryReader
							.open(FSDirectory.open(Paths.get(baseDir + "tbl_link_pop/100")))) {
				int loop = 1;
				for (ExperimentQuery query : queries) {
					System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
					Schema sch = new Schema(schemaDescription);
					List<String> articleIds = executeLuceneQuery(articleReader, query.getText());
					System.out.println(articleIds);
					List<String> imageIds = executeLuceneQuery(imageReader, query.getText());
					List<String> linkIds = executeLuceneQuery(linkReader, query.getText());
					Map<String, List<String>> relationIDs = new HashMap<String, List<String>>();
					relationIDs.put(articleTable, articleIds);
					relationIDs.put(imageTable, imageIds);
					relationIDs.put(linkTable, linkIds);
					QueryResult result = executeIRStyleQuery(jdbcacc, sch, relations, query, relationIDs);
					queryResults.add(result);
				}
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
			ExperimentQuery query, Map<String, List<String>> relationIDs) throws Exception {
		MIndexAccess MIndx = new MIndexAccess(relations);
		Vector<String> allkeyw = new Vector<String>();
		// escaping single quotes
		allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
		int exectime = 0;
		long time3 = System.currentTimeMillis();
		MIndx.createTupleSets3(sch, allkeyw, jdbcacc.conn, relationIDs);
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

	static List<String> executeLuceneQuery(IndexReader reader, String queryText) throws ParseException, IOException {
		IndexSearcher searcher = new IndexSearcher(reader);
		QueryParser qp = new QueryParser(WikiTableIndexer.TEXT_FIELD, new StandardAnalyzer());
		Query query = qp.parse(queryText);
		int n = 50;
		ScoreDoc[] scoreDocHits = searcher.search(query, n).scoreDocs;
		List<String> results = new ArrayList<String>();
		for (int j = 0; j < Math.min(n, scoreDocHits.length); j++) {
			Document doc = reader.document(scoreDocHits[j].doc);
			String docId = doc.get(WikiTableIndexer.ID_FIELD);
			results.add(docId);
		}
		return results;
	}

}
