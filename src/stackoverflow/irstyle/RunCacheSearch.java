package stackoverflow.irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import irstyle.IRStyleKeywordSearch;
import irstyle.IRStyleQueryResult;
import irstyle.api.Params;
import irstyle.core.JDBCaccess;
import irstyle.core.MIndexAccess;
import irstyle.core.Relation;
import irstyle.core.Result;
import irstyle.core.Schema;
import query.ExperimentQuery;
import stackoverflow.QuestionDAO;
import stackoverflow.StackQueryingExperiment;

public class RunCacheSearch {

	public static void main(String[] args) throws Exception {
		List<String> argsList = Arrays.asList(args);
		String cacheNameSuffix = "mrr";
		String outputFileName = "result";
		StackQueryingExperiment sqe = new StackQueryingExperiment();
		List<QuestionDAO> queries = sqe.loadQuestionsFromTable("questions_s_test_train");
		boolean justUseCache = false;
		if (argsList.contains("-cache")) {
			justUseCache = true;
			outputFileName += "_cache";
		} else {
			outputFileName += "_full";
		}
		if (argsList.contains("-eff")) {
			queries = queries.subList(0, 20);
			outputFileName += "_eff";
		}
		outputFileName += ".csv";
		JDBCaccess jdbcacc = IRStyleKeywordSearch.jdbcAccess("stack_overflow");
		IRStyleKeywordSearch.dropAllTuplesets(jdbcacc);
		List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
		try (IndexReader articleReader = DirectoryReader
				.open(FSDirectory.open(Paths.get(Constants.DATA_STACK + Constants.tableName[0] + "/100")));
				IndexReader articleCacheReader = DirectoryReader.open(FSDirectory.open(
						Paths.get(Constants.DATA_STACK + "sub_" + Constants.tableName[0] + "_" + cacheNameSuffix)));
				IndexReader imageReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(Constants.DATA_STACK + Constants.tableName[1] + "/100")));
				IndexReader imageCacheReader = DirectoryReader.open(FSDirectory.open(
						Paths.get(Constants.DATA_STACK + "sub_" + Constants.tableName[1] + "_" + cacheNameSuffix)));
				IndexReader linkReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(Constants.DATA_STACK + Constants.tableName[2] + "/100")));
				IndexReader linkCacheReader = DirectoryReader.open(FSDirectory.open(
						Paths.get(Constants.DATA_STACK + "sub_" + Constants.tableName[2] + "_" + cacheNameSuffix)))) {
			long time = 0;
			int cacheUseCount = 0;
			long selectionTime = 0;
			long luceneTime = 0;
			long tuplesetTime = 0;
			double recall = 0;
			double p20 = 0;
			for (int exec = 0; exec < Params.numExecutions; exec++) {
				int loop = 1;
				for (QuestionDAO query : queries) {
					System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.text);
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
					if (justUseCache) {
						cacheUseCount++;
						articleTable = "sub_article_wiki13";
						articleImageTable = "sub_article_image_09";
						imageTable = "sub_image_pop";
						articleLinkTable = "sub_article_link_09";
						linkTable = "sub_link_pop";
						articleIndexToUse = articleCacheReader;
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
					if (Params.DEBUG) {
						System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d", articleIds.size(),
								imageIds.size(), linkIds.size());
					}
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(articleTable, articleIds);
					relnamesValues.put(imageTable, imageIds);
					relnamesValues.put(linkTable, linkIds);
					// TODO
					IRStyleQueryResult result = RunCacheSearch.executeIRStyleQuery(jdbcacc, sch, relations, null,
							relnamesValues);
					result.dedup();
					tuplesetTime += result.tuplesetTime;
					time += luceneTime + result.execTime;
					System.out.println("rrank=" + result.rrank());
					recall += result.recall();
					p20 += result.p20();
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
			System.out.println("recall = " + recall / queries.size());
			System.out.println("p20 = " + p20 / queries.size());
			IRStyleKeywordSearch.printResults(queryResults, outputFileName);
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
		QueryParser qp = new QueryParser("???", new StandardAnalyzer());
		Query query = qp.parse(QueryParser.escape(queryText));
		ScoreDoc[] scoreDocHits = searcher.search(query, Params.MAX_TS_SIZE).scoreDocs;
		List<String> results = new ArrayList<String>();
		for (int j = 0; j < scoreDocHits.length; j++) {
			Document doc = reader.document(scoreDocHits[j].doc);
			String docId = doc.get("???");
			results.add("(" + docId + "," + scoreDocHits[j].score + ")");
		}
		return results;
	}

}
