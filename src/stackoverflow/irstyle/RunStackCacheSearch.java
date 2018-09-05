package stackoverflow.irstyle;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import irstyle.IRStyleQueryResult;
import irstyle.api.IRStyleExperiment;
import irstyle.api.IRStyleKeywordSearch;
import irstyle.api.Params;
import irstyle.core.ExecPrepared;
import irstyle.core.JDBCaccess;
import irstyle.core.Relation;
import irstyle.core.Schema;
import query.ExperimentQuery;
import stackoverflow.QuestionDAO;
import stackoverflow.StackQueryingExperiment;

public class RunStackCacheSearch {

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Option.builder("c").desc("Use cache").build());
		options.addOption(Option.builder("f").desc("Efficiency experiment").build());
		CommandLineParser clp = new DefaultParser();
		CommandLine cl = clp.parse(options, args);
		IRStyleExperiment experiment = IRStyleExperiment.createStackExperiment();
		String outputFileName = "/data/ghadakcv/stack/result";
		StackQueryingExperiment sqe = new StackQueryingExperiment();
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable("questions_s_test_train");
		List<ExperimentQuery> queries = QuestionDAO.convertToExperimentQuery(questions);
		String answersTable = StackConstants.tableName[0];
		String tagsTable = StackConstants.tableName[1];
		String commentsTable = StackConstants.tableName[2];
		String postTagsTable = StackConstants.ANSWER_TAGS_TABLE;
		String postCommentsTable = StackConstants.ANSWER_COMMENTS_TABLE;
		String answersIndexPath = experiment.dataDir + StackConstants.tableName[0] + "_full";
		String tagsIndexPath = experiment.dataDir + StackConstants.tableName[1] + "_full";
		String commentsIndexPath = experiment.dataDir + StackConstants.tableName[2] + "_full";
		if (cl.hasOption('c')) {
			outputFileName += "_cache";
			answersTable = experiment.cacheNames[0];
			tagsTable = experiment.cacheNames[1];
			commentsTable = experiment.cacheNames[2];
			answersIndexPath = experiment.dataDir + experiment.cacheNames[0];
			tagsIndexPath = experiment.dataDir + experiment.cacheNames[1];
			commentsIndexPath = experiment.dataDir + experiment.cacheNames[2];
		} else {
			outputFileName += "_full";
		}
		if (cl.hasOption('f')) {
			Collections.shuffle(queries, new Random(1));
			queries = queries.subList(0, 10000);
			outputFileName += "_eff";
		}
		outputFileName += ".csv";
		Params.MAX_TS_SIZE = 100;
		Params.N = 10;
		System.out.println("setting: \n" + Params.getDescriptor());
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
			double mrr = 0;
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
					recall += result.recall();
					p20 += result.p20();
					mrr += result.rrank();
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
			System.out.println("mrr = " + mrr / queries.size());
			System.out.println("avergae article TS size: "
					+ IRStyleKeywordSearch.aggregateArticleTuplesetSize / IRStyleKeywordSearch.counter);
			System.out.println("average gen queries: " + ExecPrepared.totalGenQueries / ExecPrepared.execCount);
			IRStyleKeywordSearch.printResults(queryResults, outputFileName);
		}
	}

}
