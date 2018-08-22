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

public class RunStackCacheSearch {

	public static void main(String[] args) throws Exception {
		List<String> argsList = Arrays.asList(args);
		String cacheNameSuffix = "mrr";
		String outputFileName = "result";
		StackQueryingExperiment sqe = new StackQueryingExperiment();
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable("questions_s_test_train");
		List<ExperimentQuery> queries = QuestionDAO.convertToExperimentQuery(questions);
		String answersTable = Constants.tableName[0];
		String tagsTable = Constants.tableName[1];
		String commentsTable = Constants.tableName[2];
		String postTagsTable = Constants.ANSWER_TAGS_TABLE;
		String postCommentsTable = Constants.ANSWER_COMMENTS_TABLE;
		String answersIndexPath = Constants.DATA_STACK + Constants.tableName[0] + "_full";
		String tagsIndexPath = Constants.DATA_STACK + Constants.tableName[1] + "_full";
		String commentsIndexPath = Constants.DATA_STACK + Constants.tableName[2] + "_full";
		if (argsList.contains("-cache")) {
			outputFileName += "_cache";
			answersTable = "sub" + answersTable;
			tagsTable = "sub" + tagsTable;
			commentsTable = "sub_" + commentsTable;
			answersIndexPath = Constants.DATA_STACK + "sub_" + Constants.tableName[0] + "_" + cacheNameSuffix;
			tagsIndexPath = Constants.DATA_STACK + "sub_" + Constants.tableName[1] + "_" + cacheNameSuffix;
			commentsIndexPath = Constants.DATA_STACK + "sub_" + Constants.tableName[2] + "_" + cacheNameSuffix;

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
		try (IndexReader answersReader = DirectoryReader.open(FSDirectory.open(Paths.get(answersIndexPath)));
				IndexReader tagsIndexReader = DirectoryReader.open(FSDirectory.open(Paths.get(tagsIndexPath)));
				IndexReader commentsIndexReader = DirectoryReader
						.open(FSDirectory.open(Paths.get(commentsIndexPath)))) {
			long time = 0;
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
					long start = System.currentTimeMillis();
					String schemaDescription = "5 " + answersTable + " " + postTagsTable + " " + tagsTable + " "
							+ postCommentsTable + " " + commentsTable + " " + answersTable + " " + postTagsTable + " "
							+ postTagsTable + " " + tagsTable + " " + answersTable + " " + postCommentsTable + " "
							+ postCommentsTable + " " + commentsTable;
					Schema sch = new Schema(schemaDescription);
					Vector<Relation> relations = IRStyleStackHelper.createRelations(answersTable, postTagsTable,
							tagsTable, postCommentsTable, commentsTable, jdbcacc.conn);
					start = System.currentTimeMillis();
					List<String> articleIds = IRStyleKeywordSearch.executeLuceneQuery(answersReader, query.getText(),
							TableIndexer.TEXT_FIELD, TableIndexer.ID_FIELD);
					List<String> imageIds = IRStyleKeywordSearch.executeLuceneQuery(tagsIndexReader, query.getText(),
							TableIndexer.TEXT_FIELD, TableIndexer.ID_FIELD);
					List<String> linkIds = IRStyleKeywordSearch.executeLuceneQuery(commentsIndexReader, query.getText(),
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
			luceneTime /= (queries.size() * Params.numExecutions);
			tuplesetTime /= (queries.size() * Params.numExecutions);
			time /= queries.size() * Params.numExecutions;
			System.out.println("average lucene time = " + luceneTime + " (ms)");
			System.out.println("average tupleset time = " + tuplesetTime + " (ms)");
			System.out.println("average just search time = " + (time - tuplesetTime) + " (ms)");
			System.out.println("average total time  = " + time + " (ms)");
			System.out.println("recall = " + recall / queries.size());
			System.out.println("p20 = " + p20 / queries.size());
			IRStyleKeywordSearch.printResults(queryResults, outputFileName);
		}
	}

}
