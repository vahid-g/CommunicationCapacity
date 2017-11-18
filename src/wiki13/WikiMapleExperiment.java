package wiki13;

import indexing.InexFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.lucene.search.similarities.BM25Similarity;

import popularity.PopularityUtils;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;

public class WikiMapleExperiment {

	private static final Logger LOGGER = Logger
			.getLogger(WikiMapleExperiment.class.getName());
	private static final String DATA_PATH = "/data/ghadakcv/";
	private static final String INDEX_PATH = DATA_PATH + "wiki_index";
	private static final String FILELIST_PATH = DATA_PATH
			+ "wiki13_count13_text.csv";
	private static final String FILELIST_COUNT09_PATH = DATA_PATH
			+ "wiki13_count09_text.csv";
	private static final String QUERY_FILE_PATH = DATA_PATH + "2013-adhoc.xml";
	private static final String QREL_FILE_PATH = DATA_PATH + "2013-adhoc.qrels";
	private static final String MSN_QUERY_FILE_PATH = DATA_PATH + "msn_query_qid.csv";
	private static final String MSN_QREL_FILE_PATH = DATA_PATH + "msn.qrels";

	public static void main(String[] args) {
		Options options = new Options();
		Option indexOption = new Option("index", false, "run indexing mode");
		options.addOption(indexOption);
		Option queryOption = new Option("query", true,
				"run querying with cache/filter");
		options.addOption(queryOption);
		Option useMsnOption = new Option("msn", false, "specifies the query log (msn/inex)");
		options.addOption(useMsnOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			cl = clp.parse(options, args);
			if (cl.hasOption("index")) {
				buildIndex(FILELIST_PATH, INDEX_PATH);
			} else if (cl.hasOption("query")) {
				String flag = cl.getOptionValue("query");
				if (flag == null) {
					throw new org.apache.commons.cli.ParseException(
							"-query needs an argument");
				}
				List<ExperimentQuery> queries;
				Map<String, Double> idPopMap;
				if (cl.hasOption("msn")) {
					queries = QueryServices.loadMsnQueries(MSN_QUERY_FILE_PATH, MSN_QREL_FILE_PATH);
					idPopMap = PopularityUtils
							.loadIdPopularityMap(FILELIST_COUNT09_PATH);
				} else {
					queries = QueryServices.loadInexQueries(
							QUERY_FILE_PATH, QREL_FILE_PATH, "title");
					idPopMap = PopularityUtils
							.loadIdPopularityMap(FILELIST_PATH);
				}
				List<QueryResult> results = Wiki13Experiment
						.runQueriesOnGlobalIndex(INDEX_PATH, queries);
				if (flag.equals("cache")) {
					QueryResult.logResultsWithPopularity(results, idPopMap,
							"before.log", 50);
					List<Double> thresholds = new ArrayList<Double>();
					List<InexFile> inexFiles = InexFile
							.loadInexFileList(FILELIST_PATH);
					for (double i = 1; i <= 50; i++) {
						int size = (int) Math.floor(inexFiles.size()
								* (i / 50.0) - 1);
						thresholds.add(inexFiles.get(size).weight);
					}
					LOGGER.log(Level.INFO, "Caching thresholds: {0}",
							thresholds);
					List<List<QueryResult>> resultsList = filterResultsWithSingleThreshold(
							results, idPopMap, thresholds);
					QueryResult.logResultsWithPopularity(resultsList.get(0),
							idPopMap, "after.log", 50);
					writeResultsListToFile(resultsList, "cache/");
				} else if (flag.equals("filter")) {
					List<List<QueryResult>> resultsList = filterResultsWithQueryThreshold(
							results, idPopMap);
					writeResultsListToFile(resultsList, "filter/");
				}
			}
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.INFO, e.getMessage());
			formatter.printHelp("", options);
			return;
		}
	}
	
	private static void buildIndex(String fileListPath,
			String indexDirectoryPath) {
		try {
			List<InexFile> pathCountList = InexFile
					.loadInexFileList(fileListPath);
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			File indexPathFile = new File(indexDirectoryPath);
			if (!indexPathFile.exists()) {
				indexPathFile.mkdirs();
			}
			LOGGER.log(Level.INFO, "Building index at: " + indexDirectoryPath);
			Wiki13Indexer.buildIndexOnText(pathCountList, indexDirectoryPath,
					new BM25Similarity());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected static List<List<QueryResult>> filterResultsWithSingleThreshold(
			List<QueryResult> results, Map<String, Double> idPopMap,
			List<Double> thresholds) {
		List<List<QueryResult>> resultsList = new ArrayList<List<QueryResult>>();
		LOGGER.log(Level.INFO, "Caching results..");
		QueryResult newResult;
		for (double x : thresholds) {
			List<QueryResult> newResults = new ArrayList<QueryResult>();
			for (QueryResult result : results) {
				newResult = filterQueryResult(result, idPopMap, x);
				newResults.add(newResult);
			}
			resultsList.add(newResults);
		}
		return resultsList;
	}
	protected static List<List<QueryResult>> filterResultsWithQueryThreshold(
			List<QueryResult> results, Map<String, Double> idPopMap) {
		List<List<QueryResult>> resultsList = new ArrayList<List<QueryResult>>();
		LOGGER.log(Level.INFO, "Filtering results..");
		QueryResult newResult;
		for (double x = 0.01; x <= 1; x += 0.01) {
			List<QueryResult> newResults = new ArrayList<QueryResult>();
			for (QueryResult result : results) {
				double threshold = findThresholdPerQuery(result, idPopMap, x);
				newResult = filterQueryResult(result, idPopMap, threshold);
				newResults.add(newResult);
			}
		}
		return resultsList;
	}

	protected static double findThresholdPerQuery(QueryResult result,
			Map<String, Double> idPopMap, double cutoffSize) {
		List<Double> pops = new ArrayList<Double>();
		for (String id : result.getTopResults()) {
			pops.add(idPopMap.get(id));
		}
		Collections.sort(pops, Collections.reverseOrder());
		double cutoffWeight = pops.get((int) Math.floor(cutoffSize
				* pops.size()) - 1);
		return cutoffWeight;
	}

	protected static QueryResult filterQueryResult(QueryResult result,
			Map<String, Double> idPopMap, double cutoffWeight) {
		QueryResult newResult = new QueryResult(result.query);
		if (result.getTopResults().size() < 2) {
			LOGGER.log(Level.WARNING, "query just has zero or one result");
			return newResult;
		}
		List<String> newTopResults = new ArrayList<String>();
		List<String> newTopResultTitles = new ArrayList<String>();
		List<String> newExplanation = new ArrayList<String>();
		for (int i = 0; i < result.getTopResults().size(); i++) {
			if (idPopMap.get(result.getTopResults().get(i)) >= cutoffWeight) {
				newTopResults.add(result.getTopResults().get(i));
				newTopResultTitles.add(result.getTopResultsTitle().get(i));
				newExplanation.add(result.getExplanations().get(i));
			}
		}
		newResult.setTopResults(newTopResults);
		newResult.setTopResultsTitle(newTopResultTitles);
		newResult.setExplanations(newExplanation);
		return newResult;
	}

	protected static void writeResultsListToFile(
			List<List<QueryResult>> resultsList, String resultDirectoryPath) {
		try (FileWriter p20Writer = new FileWriter("wiki_p20.csv");
				FileWriter mrrWriter = new FileWriter("wiki_mrr.csv");
				FileWriter rec200Writer = new FileWriter("wiki_recall200.csv");
				FileWriter recallWriter = new FileWriter("wiki_recall.csv")) {
			for (int i = 0; i < resultsList.get(0).size(); i++) {
				ExperimentQuery query = resultsList.get(0).get(i).query;
				p20Writer.write(query.getText());
				mrrWriter.write(query.getText());
				rec200Writer.write(query.getText());
				recallWriter.write(query.getText());
				for (int j = 0; j < resultsList.size(); j++) {
					QueryResult result = resultsList.get(j).get(i);
					p20Writer.write("," + result.precisionAtK(20));
					mrrWriter.write("," + result.mrr());
					rec200Writer.write("," + result.recallAtK(200));
					recallWriter.write("," + result.recallAtK(1000));
				}
				p20Writer.write("\n");
				mrrWriter.write("\n");
				rec200Writer.write("\n");
				recallWriter.write("\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public static void writeResultsToFile(List<QueryResult> results,
			String resultFilePath) {
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(resultFilePath)) {
			for (QueryResult iqr : results) {
				fw.write(iqr.resultString() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
