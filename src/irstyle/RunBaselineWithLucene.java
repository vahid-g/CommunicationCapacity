package irstyle;

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
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import irstyle_core.JDBCaccess;
import irstyle_core.MIndexAccess;
import irstyle_core.Relation;
import irstyle_core.Result;
import irstyle_core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiFilesPaths;
import wiki13.WikiRelationalEfficiencyExperiment;

public class RunBaselineWithLucene {

	static int maxCNsize = 5;
	static int numExecutions = 1;
	static int N = 100;
	static boolean allKeywInResults = false;
	static int tupleSetSize = 100000;

	public static void main(String[] args) throws Exception {
		JDBCaccess jdbcacc = jdbcAccess();
		int time = 0;
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
			Vector<Relation> relations = IRStyleKeywordSearch.createRelations(articleTable, imageTable, linkTable,
					jdbcacc.conn);
			IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
			WikiFilesPaths paths = null;
			paths = WikiFilesPaths.getMaplePaths();
			List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
					paths.getMsnQrelFilePath());
			Collections.shuffle(queries, new Random(1));
			queries = queries.subList(0, 10);
			List<QueryResult> queryResults = new ArrayList<QueryResult>();
			String baseDir = "/data/ghadakcv/wikipedia/";
			try (IndexReader articleReader = DirectoryReader
					.open(FSDirectory.open(Paths.get(baseDir + "tbl_article_wiki13/100")));
					// .open(FSDirectory.open(Paths.get(baseDir + "tbl_article_09/100")));
					IndexReader imageReader = DirectoryReader
							.open(FSDirectory.open(Paths.get(baseDir + "tbl_image_pop/100")));
					IndexReader linkReader = DirectoryReader
							.open(FSDirectory.open(Paths.get(baseDir + "tbl_link_pop/100")))) {
				int loop = 1;
				for (ExperimentQuery query : queries) {
					System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
					Schema sch = new Schema(schemaDescription);
					List<String> articleIds = executeLuceneQuery(articleReader, query.getText());
					List<String> imageIds = executeLuceneQuery(imageReader, query.getText());
					List<String> linkIds = executeLuceneQuery(linkReader, query.getText());
					System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d", articleIds.size(),
							imageIds.size(), linkIds.size());
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(articleTable, articleIds);
					relnamesValues.put(imageTable, imageIds);
					relnamesValues.put(linkTable, linkIds);
					QueryResult result = executeIRStyleQuery(jdbcacc, sch, relations, query, relnamesValues);
					time += result.execTime;
					queryResults.add(result);
				}
			}
			System.out.println("average time per query = " + (time / (queries.size() * numExecutions)));
			// printResults(queryResults, "ir_result.csv");
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

	static QueryResult executeIRStyleQuery(JDBCaccess jdbcacc, Schema sch, Vector<Relation> relations,
			ExperimentQuery query, Map<String, List<String>> relnameValues) throws Exception {
		MIndexAccess MIndx = new MIndexAccess(relations);
		Vector<String> allkeyw = new Vector<String>();
		// escaping single quotes
		allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
		int exectime = 0;
		long time3 = System.currentTimeMillis();
		MIndx.createTupleSets3(sch, allkeyw, jdbcacc.conn, relnameValues);
		long time4 = System.currentTimeMillis();
		exectime += time4 - time3;
		System.out.println(" Time to create tuple sets: " + (time4 - time3) + " (ms)");
		for (Relation rel : relations)
			System.out.println(DatabaseHelper.tableSize("TS_" + rel.getName(), jdbcacc.conn));
		time3 = System.currentTimeMillis();
		Vector<?> CNs = sch.getCNs(maxCNsize, allkeyw, sch, MIndx);
		time4 = System.currentTimeMillis();
		exectime += time4 - time3;
		System.out.println(" #CNs=" + CNs.size() + " Time to get CNs=" + (time4 - time3) + " (ms)");
		ArrayList<Result> results = new ArrayList<Result>(1);
		exectime += IRStyleKeywordSearch.methodC(N, allKeywInResults, relations, allkeyw, CNs, results, jdbcacc);
		IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
		double rrank = IRStyleKeywordSearch.rrank(results, query);
		System.out.println(" R-rank = " + rrank);
		QueryResult result = new QueryResult(query, rrank, exectime);
		return result;
	}

	static List<String> executeLuceneQuery(IndexReader reader, String queryText) throws ParseException, IOException {
		IndexSearcher searcher = new IndexSearcher(reader);
		searcher.setSimilarity(new BM25Similarity());
		QueryParser qp = new QueryParser(RunTableIndexer.TEXT_FIELD, new StandardAnalyzer());
		Query query = qp.parse(queryText);
		ScoreDoc[] scoreDocHits = searcher.search(query, tupleSetSize).scoreDocs;
		List<String> results = new ArrayList<String>();
		for (int j = 0; j < scoreDocHits.length; j++) {
			Document doc = reader.document(scoreDocHits[j].doc);
			String docId = doc.get(RunTableIndexer.ID_FIELD);
			results.add("(" + docId + "," + scoreDocHits[j].score + ")");
		}
		return results;
	}

}
