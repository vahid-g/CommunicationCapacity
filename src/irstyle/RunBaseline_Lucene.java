package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class RunBaseline_Lucene {

	private static final int MAX_TS = 10000;

	public static void main(String[] args) throws Exception {
		List<String> argsList = Arrays.asList(args);
		String articleTable = "tbl_article_wiki13";
		String imageTable = "tbl_image_pop";
		String linkTable = "tbl_link_pop";
		String articleImageTable = "tbl_article_image_09";
		String articleLinkTable = "tbl_article_link_09";
		String articleIndexPath = Indexer.DATA_WIKIPEDIA + "tbl_article_wiki13/100";
		String imageIndexPath = Indexer.DATA_WIKIPEDIA + "tbl_image_pop/100";
		String linkIndexPath = Indexer.DATA_WIKIPEDIA + "tbl_link_pop/100";
		if (argsList.contains("-debug")) {
			Params.DEBUG = true;
		}
		if (argsList.contains("-cache")) {
			articleTable = "sub_article_wiki13";
			imageTable = "sub_image_pop";
			linkTable = "sub_link_pop";
			articleImageTable = "sub_article_image_09";
			articleLinkTable = "sub_article_link_09";
			articleIndexPath = Indexer.DATA_WIKIPEDIA + "sub_article_wiki13";
			imageIndexPath = Indexer.DATA_WIKIPEDIA + "sub_image_pop";
			linkIndexPath = Indexer.DATA_WIKIPEDIA + "sub_link_pop";
		}
		List<ExperimentQuery> queries = null;
		if (argsList.contains("-inex")) {
			queries = QueryServices.loadInexQueries();
		} else {
			queries = QueryServices.loadMsnQueries();
		}
		Collections.shuffle(queries, new Random(1));
		queries = queries.subList(0, 10);
		List<IRStyleQueryResult> queryResults = runExperiment(queries, articleTable, articleImageTable, imageTable,
				articleLinkTable, linkTable, articleIndexPath, imageIndexPath, linkIndexPath);
		IRStyleKeywordSearch.printResults(queryResults, "result.csv");
	}

	static List<IRStyleQueryResult> runExperiment(List<ExperimentQuery> queries, String articleTable,
			String articleImageTable, String imageTable, String articleLinkTable, String linkTable,
			String articleIndexPath, String imageIndexPath, String linkIndexPath) throws Exception {
		JDBCaccess jdbcacc = IRStyleKeywordSearch.jdbcAccess();
		String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
				+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
				+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
				+ articleLinkTable + " " + linkTable;
		Vector<Relation> relations = IRStyleKeywordSearch.createRelations(articleTable, imageTable, linkTable,
				articleImageTable, articleLinkTable, jdbcacc.conn);
		IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);

		List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
		try (IndexReader articleReader = DirectoryReader.open(FSDirectory.open(Paths.get(articleIndexPath)));
				IndexReader imageReader = DirectoryReader.open(FSDirectory.open(Paths.get(imageIndexPath)));
				IndexReader linkReader = DirectoryReader.open(FSDirectory.open(Paths.get(linkIndexPath)))) {
			int time = 0;
			long luceneTime = 0;
			long tuplesetTime = 0;
			for (int exec = 0; exec < Params.numExecutions; exec++) {
				int loop = 1;
				for (ExperimentQuery query : queries) {
					System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
					Schema sch = new Schema(schemaDescription);
					long start = System.currentTimeMillis();
					List<String> articleIds = executeLuceneQuery(articleReader, query.getText());
					List<String> imageIds = executeLuceneQuery(imageReader, query.getText());
					List<String> linkIds = executeLuceneQuery(linkReader, query.getText());
					long end = System.currentTimeMillis() - start;
					luceneTime += end;
					System.out.println(" Lucene search time: " + end + " (ms)");
					System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d\n", articleIds.size(),
							imageIds.size(), linkIds.size());
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(articleTable, articleIds);
					relnamesValues.put(imageTable, imageIds);
					relnamesValues.put(linkTable, linkIds);
					IRStyleQueryResult result = executeIRStyleQuery(jdbcacc, sch, relations, query, relnamesValues);
					System.out.println(" Execute IRstyle time: " + result.execTime + "(ms)");
					tuplesetTime += result.tuplesetTime;
					time += luceneTime + result.execTime;
					queryResults.add(result);
				}
			}
			luceneTime /= (queries.size() * Params.numExecutions);
			tuplesetTime /= (queries.size() * Params.numExecutions);
			time /= queries.size() * Params.numExecutions;
			System.out.println("average lucene time = " + luceneTime + " (ms)");
			System.out.println("average tupleset time = " + tuplesetTime + " (ms)");
			System.out.println("average just search time = " + (time - tuplesetTime) + " (ms)");
			System.out.println("average total time  = " + time + " (ms)");
		}
		return queryResults;
	}

	static IRStyleQueryResult executeIRStyleQuery(JDBCaccess jdbcacc, Schema sch, Vector<Relation> relations,
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
		int time = IRStyleKeywordSearch.methodC(Params.N, Params.allKeywInResults, relations, allkeyw, CNs, results,
				jdbcacc);
		exectime += time;
		if (Params.DEBUG)
			System.out.println(" Time to search joint tuplesets: " + time);
		IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
		IRStyleQueryResult result = new IRStyleQueryResult(query, exectime);
		result.addIRStyleResults(results);
		result.tuplesetTime = tuplesetTime;
		if (Params.DEBUG)
			System.out.println(" R-rank = " + result.rrank());
		return result;
	}

	static List<String> executeLuceneQuery(IndexReader reader, String queryText) throws ParseException, IOException {
		IndexSearcher searcher = new IndexSearcher(reader);
		searcher.setSimilarity(new BM25Similarity());
		QueryParser qp = new QueryParser(Indexer.TEXT_FIELD, new StandardAnalyzer());
		Query query = qp.parse(QueryParser.escape(queryText));
		ScoreDoc[] scoreDocHits = searcher.search(query, MAX_TS).scoreDocs;
		List<String> results = new ArrayList<String>();
		for (int j = 0; j < scoreDocHits.length; j++) {
			Document doc = reader.document(scoreDocHits[j].doc);
			String docId = doc.get(Indexer.ID_FIELD);
			results.add("(" + docId + "," + scoreDocHits[j].score + ")");
		}
		return results;
	}

}