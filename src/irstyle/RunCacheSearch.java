package irstyle;

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
import org.apache.lucene.queryparser.flexible.standard.parser.ParseException;
import org.apache.lucene.store.FSDirectory;

import irstyle.api.IRStyleExperimentHelper;
import irstyle.api.IRStyleExperiment;
import irstyle.api.IRStyleKeywordSearch;
import irstyle.api.Indexer;
import irstyle.api.Params;
import irstyle.core.ExecPrepared;
import irstyle.core.Relation;
import irstyle.core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import stackoverflow.QuestionDAO;
import stackoverflow.StackQueryingExperiment;

public class RunCacheSearch {

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Option.builder("e").hasArg().desc("The experiment inexp/inexr/mrr").build());
		options.addOption(Option.builder("c").desc("Use cache").build());
		options.addOption(Option.builder("f").desc("Efficiency experiment").hasArg().build());
		options.addOption(Option.builder("k").desc("The k in tok-k").hasArg().build());
		options.addOption(Option.builder("t").desc("TS size threshold").hasArg().build());
		options.addOption(Option.builder("s").desc("Score thresholding").build());
		options.addOption(Option.builder("d").desc("Output debug info").build());
		CommandLineParser clp = new DefaultParser();
		CommandLine cl = clp.parse(options, args);
		String cacheNameSuffix;
		List<ExperimentQuery> queries;
		String outputFileName = "result";
		IRStyleExperiment experiment;
		IRStyleExperimentHelper experimentHelper;
		if (cl.getOptionValue('e').equals("inexp")) {
			experiment = IRStyleExperiment.createWikiP20Experiment();
			experimentHelper = new Wiki_ExperimentHelper();
			cacheNameSuffix = "p20";
			queries = QueryServices.loadInexQueries();
		} else if (cl.getOptionValue('e').equals("inexr")) {
			experiment = IRStyleExperiment.createWikiRecExperiment();
			experimentHelper = new Wiki_ExperimentHelper();
			if (!cl.hasOption('f')) {
				Params.N = 100;
			}
			cacheNameSuffix = "rec";
			queries = QueryServices.loadInexQueries();
		} else if (cl.getOptionValue('e').equals("msn")) {
			experiment = IRStyleExperiment.createWikiMsnExperiment();
			experimentHelper = new Wiki_ExperimentHelper();
			cacheNameSuffix = "mrr";
			queries = QueryServices.loadMsnQueriesAll();
		} else if (cl.getOptionValue('e').equals("stack")) {
			outputFileName = "/data/ghadakcv/stack/result";
			experiment = IRStyleExperiment.createStackExperiment();
			experimentHelper = new Stack_ExperimentHelper();
			cacheNameSuffix = "mrr";
			StackQueryingExperiment sqe = new StackQueryingExperiment();
			List<QuestionDAO> questions = sqe.loadQuestionsFromTable("questions_s_test_train");
			queries = QuestionDAO.convertToExperimentQuery(questions);
		} else {
			throw new ParseException();
		}
		outputFileName += "_" + cacheNameSuffix;
		String[] indexPath = new String[experiment.tableNames.length];
		String[] tableNames = new String[experiment.tableNames.length];
		String[] relationTableNames = new String[2];
		if (cl.hasOption('c')) {
			outputFileName += "_cache";
			indexPath[0] = experiment.dataDir + experiment.cacheNames[0];
			indexPath[1] = experiment.dataDir + experiment.cacheNames[1];
			indexPath[2] = experiment.dataDir + experiment.cacheNames[2];
			tableNames = experiment.cacheNames;
			relationTableNames = experiment.relationCacheNames;
		} else {
			outputFileName += "_full";
			indexPath[0] = experiment.dataDir + experiment.tableNames[0] + "_full";
			indexPath[1] = experiment.dataDir + experiment.tableNames[1] + "_full";
			indexPath[2] = experiment.dataDir + experiment.tableNames[2] + "_full";
			tableNames = experiment.tableNames;
			relationTableNames = experiment.relationTableNames;
		}
		if (cl.hasOption('f')) {
			int queriesSize = Integer.parseInt(cl.getOptionValue('f', "20"));
			Collections.shuffle(queries, new Random(1));
			queries = queries.subList(0, queriesSize);
			outputFileName += "_eff";
		}
		if (cl.hasOption('d')) {
			Params.DEBUG = true;
		}
		outputFileName += ".csv";
		Params.MAX_TS_SIZE = Integer.parseInt(cl.getOptionValue("t", Integer.toString(Params.MAX_TS_SIZE)));
		Params.N = Integer.parseInt(cl.getOptionValue("k", Integer.toString(Params.N)));
		Params.useScoreThresholding = cl.hasOption("s");
		System.out.println("setting: \n" + Params.getDescriptor());
		System.out.println("output file: " + outputFileName);
		System.out.println("queries size: " + queries.size());
		IRStyleKeywordSearch.dropAllTuplesets(experimentHelper.getJdbcAccess());
		List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
		try (IndexReader articleReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath[0])));
				IndexReader imageReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath[1])));
				IndexReader linkReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath[2])))) {
			long time = 0;
			int cacheUseCount = 0;
			long selectionTime = 0;
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
					String schemaDescription = "5 " + tableNames[0] + " " + relationTableNames[0] + " " + tableNames[1]
							+ " " + relationTableNames[1] + " " + tableNames[2] + " " + tableNames[0] + " "
							+ relationTableNames[0] + " " + relationTableNames[0] + " " + tableNames[1] + " "
							+ tableNames[0] + " " + relationTableNames[1] + " " + relationTableNames[1] + " "
							+ tableNames[2];
					Schema sch = new Schema(schemaDescription);
					if (Params.DEBUG) {
						System.out.println(" Using tables: " + tableNames[0] + " " + relationTableNames[0] + " "
								+ tableNames[1] + " " + relationTableNames[1] + " " + tableNames[2]);
						System.out.println(" Using indices: " + indexPath[0] + " " + indexPath[1] + " " + indexPath[2]);
					}
					Vector<Relation> relations;
					relations = experimentHelper.createRelations(tableNames[0], tableNames[1], tableNames[2],
							relationTableNames[0], relationTableNames[1]);
					long start = System.currentTimeMillis();
					List<String> articleIds = IRStyleKeywordSearch.executeLuceneQuery(articleReader, query.getText(),
							Indexer.TEXT_FIELD, Indexer.ID_FIELD);
					List<String> imageIds = IRStyleKeywordSearch.executeLuceneQuery(imageReader, query.getText(),
							Indexer.TEXT_FIELD, Indexer.ID_FIELD);
					List<String> linkIds = IRStyleKeywordSearch.executeLuceneQuery(linkReader, query.getText(),
							Indexer.TEXT_FIELD, Indexer.ID_FIELD);
					luceneTime += (System.currentTimeMillis() - start);
					if (Params.DEBUG) {
						System.out.printf(" |TS_0| = %d |TS_1| = %d |TS_2| = %d", articleIds.size(), imageIds.size(),
								linkIds.size());
					}
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(tableNames[0], articleIds);
					relnamesValues.put(tableNames[1], imageIds);
					relnamesValues.put(tableNames[2], linkIds);
					IRStyleQueryResult result = IRStyleKeywordSearch.executeIRStyleQuery(
							experimentHelper.getJdbcAccess(), sch, relations, query, relnamesValues);
					if (Params.DEBUG) {
						System.out.println(" table scan percentage = " + (double) ExecPrepared.lastGenQueries
								/ (articleIds.size() * imageIds.size() * linkIds.size()) + "%");
					}
					result.dedup();
					tuplesetTime += result.tuplesetTime;
					time += luceneTime + result.execTime;
					recall += result.recall();
					p20 += result.p20();
					mrr += result.rrank();
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
			System.out.println("mrr = " + mrr / queries.size());
			System.out.println("avergae article TS size: "
					+ IRStyleKeywordSearch.aggregateArticleTuplesetSize / IRStyleKeywordSearch.counter);
			System.out.println("average gen queries: " + ExecPrepared.totalGenQueries / ExecPrepared.execCount);
			IRStyleKeywordSearch.printResults(queryResults, outputFileName);
		}
	}

}
