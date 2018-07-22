package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import cache_selection.FeatureExtraction;
import irstyle_core.JDBCaccess;
import irstyle_core.Relation;
import irstyle_core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiFileIndexer;
import wiki13.WikiFilesPaths;

public class RunCacheSearchWithLucene {

	public static void main(String[] args) throws Exception {
		JDBCaccess jdbcacc = RunBaseline.jdbcAccess();
		IRStyleKeywordSearch.dropAllTuplesets(jdbcacc);
		WikiFilesPaths paths = null;
		paths = WikiFilesPaths.getMaplePaths();
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
				paths.getMsnQrelFilePath());
		Collections.shuffle(queries, new Random(1));
		queries = queries.subList(0, 10);
		// queries = queries.subList(1, 2);
		List<QueryResult> queryResults = new ArrayList<QueryResult>();
		String baseDir = "/data/ghadakcv/wikipedia/";
		try (IndexReader articleReader = DirectoryReader
				.open(FSDirectory.open(Paths.get(baseDir + "tbl_article_wiki13/100")));
				IndexReader articleCacheReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(baseDir + "tbl_article_wiki13/1")));
				IndexReader articleRestReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(baseDir + "tbl_article_wiki13/c1")));
				IndexReader imageReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(baseDir + "tbl_image_pop/100")));
				IndexReader imageCacheReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(baseDir + "tbl_image_pop/10")));
				IndexReader imageRestReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(baseDir + "tbl_image_pop/c10")));
				IndexReader linkReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(baseDir + "tbl_link_pop/100")));
				IndexReader linkCacheReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(baseDir + "tbl_link_pop/6")));
				IndexReader linkRestReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(baseDir + "tbl_link_pop/c6")))) {
			long time = 0;
			for (int exec = 0; exec < RunBaselineWithLucene.numExecutions; exec++) {
				int loop = 1;
				for (ExperimentQuery query : queries) {
					System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
					Vector<String> allkeyw = new Vector<String>();
					// escaping single quotes
					allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
					// String articleTable = "tbl_article_09";
					String articleTable = "tbl_article_wiki13";
					String imageTable = "tbl_image_09_tk";
					String linkTable = "tbl_link_09";
					String articleImageTable = "tbl_article_image_09";
					String articleLinkTable = "tbl_article_link_09";
					IndexReader articleIndexToUse = articleReader;
					IndexReader imageIndexToUse = imageReader;
					IndexReader linkIndexToUse = linkReader;
					long time1 = System.currentTimeMillis();
					if (useCache(query.getText(), articleCacheReader, articleReader, articleRestReader)) {
						System.out.println(" using cache for articles");
						articleTable = "sub_wiki13_1";
						articleIndexToUse = articleCacheReader;
					}
					if (useCache(query.getText(), imageCacheReader, imageReader, imageRestReader)) {
						System.out.println(" using cache for images");
						imageTable = "sub_image_10";
						imageIndexToUse = imageCacheReader;
					}
					if (useCache(query.getText(), linkCacheReader, linkReader, linkRestReader)) {
						System.out.println(" using cache for links");
						linkTable = "sub_link_6";
						linkIndexToUse = linkCacheReader;
					}
					long time2 = System.currentTimeMillis();
					System.out.println(" Time to select cache: " + (time2 - time1) + " (ms)");
					String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
							+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
							+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
							+ articleLinkTable + " " + linkTable;
					Schema sch = new Schema(schemaDescription);
					Vector<Relation> relations = IRStyleKeywordSearch.createRelations(articleTable, imageTable,
							linkTable, jdbcacc.conn);
					List<String> articleIds = RunBaselineWithLucene.executeLuceneQuery(articleIndexToUse,
							query.getText());
					List<String> imageIds = RunBaselineWithLucene.executeLuceneQuery(imageIndexToUse, query.getText());
					List<String> linkIds = RunBaselineWithLucene.executeLuceneQuery(linkIndexToUse, query.getText());
					System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d", articleIds.size(),
							imageIds.size(), linkIds.size());
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(articleTable, articleIds);
					relnamesValues.put(imageTable, imageIds);
					relnamesValues.put(linkTable, linkIds);
					QueryResult result = RunBaselineWithLucene.executeIRStyleQuery(jdbcacc, sch, relations, query,
							relnamesValues);
					time += result.execTime;
					queryResults.add(result);
				}
			}
			System.out.println(
					"average time per query = " + (time / (queries.size() * RunBaselineWithLucene.numExecutions)));
			IRStyleKeywordSearch.printResults(queryResults, "cs_result.csv");
		}
	}

	static boolean useCache(String query, IndexReader cacheIndexReader, IndexReader globalIndexReader,
			IndexReader restIndexReader) throws IOException {
		FeatureExtraction fe = new FeatureExtraction(WikiFileIndexer.WEIGHT_ATTRIB);
		double ql_cache = 0;
		double ql_rest = 0;
		ql_cache = fe.queryLikelihood(cacheIndexReader, query, RunTableIndexer.TEXT_FIELD, globalIndexReader,
				new StandardAnalyzer());
		ql_rest = fe.queryLikelihood(restIndexReader, query, RunTableIndexer.TEXT_FIELD, globalIndexReader,
				new StandardAnalyzer());
		return (ql_cache >= ql_rest);
		// return false;
	}

}
