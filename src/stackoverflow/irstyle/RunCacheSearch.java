package stackoverflow.irstyle;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import irstyle.IRStyleQueryResult;
import irstyle.api.IRStyleKeywordSearch;
import irstyle.api.Params;
import irstyle.core.JDBCaccess;
import irstyle.core.Relation;
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
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable("questions_s_test_train");
		List<ExperimentQuery> queries = QuestionDAO.convertToExperimentQuery(questions);
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
				for (ExperimentQuery query : queries) {
					System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
					Vector<String> allkeyw = new Vector<String>();
					// escaping single quotes
					allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
					String answersTable = Constants.tableName[0];
					String tagsTable = Constants.tableName[1];
					String commentsTable = Constants.tableName[2];
					IndexReader articleIndexToUse = articleReader;
					IndexReader imageIndexToUse = imageReader;
					IndexReader linkIndexToUse = linkReader;
					long start = System.currentTimeMillis();
					if (justUseCache) {
						cacheUseCount++;
						answersTable = "sub" + answersTable;
						tagsTable = "sub" + tagsTable;
						commentsTable = "sub_" + commentsTable;
						articleIndexToUse = articleCacheReader;
						imageIndexToUse = imageCacheReader;
						linkIndexToUse = linkCacheReader;
					}
					selectionTime += System.currentTimeMillis() - start;
					String schemaDescription = "3 " + answersTable + " " + tagsTable + " " + commentsTable + " "
							+ answersTable + " " + " " + tagsTable + " " + answersTable + " " + commentsTable;
					Schema sch = new Schema(schemaDescription);
					Vector<Relation> relations = IRStyleStackHelper.createRelations(answersTable, tagsTable,
							commentsTable, jdbcacc.conn);
					start = System.currentTimeMillis();
					List<String> articleIds = IRStyleKeywordSearch.executeLuceneQuery(articleIndexToUse,
							query.getText(), TableIndexer.TEXT_FIELD, TableIndexer.ID_FIELD);
					List<String> imageIds = IRStyleKeywordSearch.executeLuceneQuery(imageIndexToUse, query.getText(),
							TableIndexer.TEXT_FIELD, TableIndexer.ID_FIELD);
					List<String> linkIds = IRStyleKeywordSearch.executeLuceneQuery(linkIndexToUse, query.getText(),
							TableIndexer.TEXT_FIELD, TableIndexer.ID_FIELD);
					luceneTime += (System.currentTimeMillis() - start);
					if (Params.DEBUG) {
						System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d", articleIds.size(),
								imageIds.size(), linkIds.size());
					}
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(answersTable, articleIds);
					relnamesValues.put(tagsTable, imageIds);
					relnamesValues.put(commentsTable, linkIds);
					IRStyleQueryResult result = IRStyleKeywordSearch.executeIRStyleQuery(jdbcacc, sch, relations, query,
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

}
