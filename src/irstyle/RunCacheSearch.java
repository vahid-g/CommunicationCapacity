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

import irstyle.core.JDBCaccess;
import irstyle.core.MIndexAccess;
import irstyle.core.Relation;
import irstyle.core.Result;
import irstyle.core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiFilesPaths;

public class RunCacheSearch {

	static final int MAX_TS_SIZE = 1000;

	public static void main(String[] args) throws Exception {
		String cacheNameSuffix = args[0];
		if (cacheNameSuffix.equals("rec")) {
			Params.N = 100;
		} else if (cacheNameSuffix.equals("mrr")) {
			Params.N = 5;
		} else if (cacheNameSuffix.equals("p20")) {
			Params.N = 20;
		}
		List<String> argsList = Arrays.asList(args);
		JDBCaccess jdbcacc = IRStyleKeywordSearch.jdbcAccess();
		IRStyleKeywordSearch.dropAllTuplesets(jdbcacc);
		WikiFilesPaths paths = null;
		paths = WikiFilesPaths.getMaplePaths();
		List<ExperimentQuery> queries = null;
		if (argsList.contains("-debug")) {
			Params.DEBUG = true;
		}
		if (argsList.contains("-inex")) {
			queries = QueryServices.loadInexQueries(paths.getInexQueryFilePath(), paths.getInexQrelFilePath());
		} else {
			queries = QueryServices.loadMsnQueriesAll();
			Collections.shuffle(queries, new Random(1));
		}
		boolean justUseCache = false;
		boolean useQueryLikelihood = false;
		if (argsList.contains("-cache")) {
			justUseCache = true;
		} else if (argsList.contains("-ql")) {
			useQueryLikelihood = true;
		}

		List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
		try (IndexReader articleReader = DirectoryReader
				.open(FSDirectory.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "tbl_article_wiki13/100")));
				IndexReader articleCacheReader = DirectoryReader.open(FSDirectory.open(
						Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "sub_article_wiki13_" + cacheNameSuffix)));
				IndexReader imageReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "tbl_image_pop/100")));
				IndexReader imageCacheReader = DirectoryReader.open(FSDirectory
						.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "sub_image_pop_" + cacheNameSuffix)));
				IndexReader linkReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "tbl_link_pop/100")));
				IndexReader linkCacheReader = DirectoryReader.open(FSDirectory
						.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "sub_link_pop_" + cacheNameSuffix)));
				IndexReader cacheReader = DirectoryReader.open(FSDirectory
						.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "lm_cache_" + cacheNameSuffix)));
				IndexReader restReader = DirectoryReader.open(FSDirectory
						.open(Paths.get(RelationalWikiIndexer.DATA_WIKIPEDIA + "lm_rest_" + cacheNameSuffix)))) {
			long time = 0;
			int cacheUseCount = 0;
			long selectionTime = 0;
			long luceneTime = 0;
			long tuplesetTime = 0;
			for (int exec = 0; exec < Params.numExecutions; exec++) {
				int loop = 1;
				for (ExperimentQuery query : queries) {
					System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
					Vector<String> allkeyw = new Vector<String>();
					// escaping single quotes
					allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
					String articleTable = "tbl_article_wiki13";
					String imageTable = "tbl_image_pop";
					String linkTable = "tbl_link_pop";
					String articleImageTable = "tbl_article_image_09";
					String articleLinkTable = "tbl_article_link_09";
					IndexReader articleIndexToUse = articleReader;
					IndexReader imageIndexToUse = imageReader;
					IndexReader linkIndexToUse = linkReader;
					long start = System.currentTimeMillis();
					if (justUseCache || (useQueryLikelihood
							&& CacheSelectionQL.useCache(query.getText(), cacheReader, articleReader, restReader))) {
						cacheUseCount++;
						articleTable = "sub_article_wiki13";
						articleImageTable = "sub_article_image_09";
						imageTable = "sub_image_pop";
						articleLinkTable = "sub_article_link_09";
						linkTable = "sub_link_pop";
						articleIndexToUse = cacheReader;
						imageIndexToUse = imageCacheReader;
						linkIndexToUse = linkCacheReader;
					}
					selectionTime += System.currentTimeMillis() - start;
					String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
							+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
							+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
							+ articleLinkTable + " " + linkTable;
					Schema sch = new Schema(schemaDescription);
					Vector<Relation> relations = IRStyleKeywordSearch.createRelations(articleTable, imageTable,
							linkTable, articleImageTable, articleLinkTable, jdbcacc.conn);
					start = System.currentTimeMillis();
					List<String> articleIds = RunCacheSearch.executeLuceneQuery(articleIndexToUse, query.getText());
					List<String> imageIds = RunCacheSearch.executeLuceneQuery(imageIndexToUse, query.getText());
					List<String> linkIds = RunCacheSearch.executeLuceneQuery(linkIndexToUse, query.getText());
					luceneTime += (System.currentTimeMillis() - start);
					System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d", articleIds.size(),
							imageIds.size(), linkIds.size());
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(articleTable, articleIds);
					relnamesValues.put(imageTable, imageIds);
					relnamesValues.put(linkTable, linkIds);
					IRStyleQueryResult result = RunCacheSearch.executeIRStyleQuery(jdbcacc, sch, relations, query,
							relnamesValues);
					tuplesetTime += result.tuplesetTime;
					time += luceneTime + result.execTime;
					queryResults.add(result);
				}
			}
			selectionTime /= (queries.size() * Params.numExecutions);
			luceneTime /= (queries.size() * Params.numExecutions);
			tuplesetTime /= (queries.size() * Params.numExecutions);
			time /= queries.size() * Params.numExecutions;
			System.out.println("average cache selection time = " + selectionTime + " (ms)");
			System.out.println("average lucene time = " + luceneTime + " (ms)");
			System.out.println("average tupleset time = " + tuplesetTime + " (ms)");
			System.out.println("average just search time = " + (time - tuplesetTime) + " (ms)");
			System.out.println("average total time  = " + time + " (ms)");
			System.out.println("number of cache hits: " + cacheUseCount + "/" + queries.size());
			IRStyleKeywordSearch.printResults(queryResults, "result.csv");
		}

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

	public static List<String> executeLuceneQuery(IndexReader reader, String queryText)
			throws ParseException, IOException {
		IndexSearcher searcher = new IndexSearcher(reader);
		searcher.setSimilarity(new BM25Similarity());
		QueryParser qp = new QueryParser(RelationalWikiIndexer.TEXT_FIELD, new StandardAnalyzer());
		Query query = qp.parse(QueryParser.escape(queryText));
		ScoreDoc[] scoreDocHits = searcher.search(query, RunCacheSearch.MAX_TS_SIZE).scoreDocs;
		List<String> results = new ArrayList<String>();
		for (int j = 0; j < scoreDocHits.length; j++) {
			Document doc = reader.document(scoreDocHits[j].doc);
			String docId = doc.get(RelationalWikiIndexer.ID_FIELD);
			results.add("(" + docId + "," + scoreDocHits[j].score + ")");
		}
		return results;
	}

}
