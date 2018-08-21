package irstyle.old;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import irstyle.IRStyleQueryResult;
import irstyle.IRStyleWikiHelper;
import irstyle.RelationalWikiIndexer;
import irstyle.api.IRStyleKeywordSearch;
import irstyle.api.Params;
import irstyle.core.JDBCaccess;
import irstyle.core.Relation;
import irstyle.core.Schema;
import query.ExperimentQuery;
import query.QueryServices;

public class RunCacheSearch_Old {

	public static void main(String[] args) throws Exception {
		List<String> argsList = Arrays.asList(args);
		String articleTable = "tbl_article_wiki13";
		String imageTable = "tbl_image_pop";
		String linkTable = "tbl_link_pop";
		String articleImageTable = "tbl_article_image_09";
		String articleLinkTable = "tbl_article_link_09";
		String articleIndexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "tbl_article_wiki13/100";
		String imageIndexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "tbl_image_pop/100";
		String linkIndexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "tbl_link_pop/100";
		if (argsList.contains("-debug")) {
			Params.DEBUG = true;
		}
		if (argsList.contains("-cache")) {
			articleTable = "sub_article_wiki13";
			imageTable = "sub_image_pop";
			linkTable = "sub_link_pop";
			articleImageTable = "sub_article_image_09";
			articleLinkTable = "sub_article_link_09";
			articleIndexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "sub_article_wiki13";
			imageIndexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "sub_image_pop";
			linkIndexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "sub_link_pop";
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
		JDBCaccess jdbcacc = IRStyleWikiHelper.jdbcAccess();
		String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
				+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
				+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
				+ articleLinkTable + " " + linkTable;
		Vector<Relation> relations = IRStyleWikiHelper.createRelations(articleTable, imageTable, linkTable,
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
					List<String> articleIds = IRStyleKeywordSearch.executeLuceneQuery(articleReader, query.getText(),RelationalWikiIndexer.TEXT_FIELD, RelationalWikiIndexer.ID_FIELD);
					List<String> imageIds = IRStyleKeywordSearch.executeLuceneQuery(imageReader, query.getText(),RelationalWikiIndexer.TEXT_FIELD, RelationalWikiIndexer.ID_FIELD);
					List<String> linkIds = IRStyleKeywordSearch.executeLuceneQuery(linkReader, query.getText(), RelationalWikiIndexer.TEXT_FIELD, RelationalWikiIndexer.ID_FIELD);
					long end = System.currentTimeMillis() - start;
					luceneTime += end;
					System.out.println(" Lucene search time: " + end + " (ms)");
					System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d\n", articleIds.size(),
							imageIds.size(), linkIds.size());
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(articleTable, articleIds);
					relnamesValues.put(imageTable, imageIds);
					relnamesValues.put(linkTable, linkIds);
					IRStyleQueryResult result = IRStyleKeywordSearch.executeIRStyleQuery(jdbcacc, sch, relations, query, relnamesValues);
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

}
