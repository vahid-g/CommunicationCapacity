package wiki13;

import indexing.InexFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
	private static final String QUERY_FILE_PATH = DATA_PATH + "2013-adhoc.xml";
	private static final String QREL_FILE_PATH = DATA_PATH + "2013-adhoc.qrels";

	public static void main(String[] args) {
		Options options = new Options();
		Option indexOption = new Option("index", false, "run indexing mode");
		options.addOption(indexOption);
		Option queryOption = new Option("query", true,
				"run querying with cache/filter");
		options.addOption(queryOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			cl = clp.parse(options, args);
			if (cl.hasOption("i")) {
				buildIndex(FILELIST_PATH, INDEX_PATH);
			} else if (cl.hasOption("q")) {
				String flag = cl.getOptionValue("q");
				if (flag == null) {
					throw new org.apache.commons.cli.ParseException(
							"-q needs an argument");
				} else if (flag.equals("cache")) {
					List<QueryResult> results = runQueriesOnGlobalIndex(
							INDEX_PATH, QUERY_FILE_PATH, QREL_FILE_PATH);
					Map<String, Double> idPopMap = PopularityUtils
							.loadIdPopularityMap(FILELIST_PATH);
					QueryResult.logResultsWithPopularity(results, idPopMap,
							"initial_ret.log");
					List<Double> thresholds = new ArrayList<Double>();
					List<InexFile> inexFiles = InexFile
							.loadInexFileList(FILELIST_PATH);
					for (double i = 0.01; i <= 1; i++)
						thresholds.add(inexFiles.get((int) Math.floor(inexFiles
								.size() * i - 1)).weight);
					List<List<QueryResult>> resultsList = filterResultsWithSingleThreshold(
							results, idPopMap, thresholds);
					writeResultsListToFile(resultsList, "cache/");
				} else if (flag.equals("filter")) {
					List<QueryResult> results = runQueriesOnGlobalIndex(
							INDEX_PATH, QUERY_FILE_PATH, QREL_FILE_PATH);
					Map<String, Double> idPopMap = PopularityUtils
							.loadIdPopularityMap(FILELIST_PATH);
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

	private static List<QueryResult> runQueriesOnGlobalIndex(String indexPath,
			String queriesFilePath, String qrelsFilePath) {
		LOGGER.log(Level.INFO, "Loading queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				queriesFilePath, qrelsFilePath);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		fieldToBoost.put(Wiki13Indexer.TITLE_ATTRIB, 0.15f);
		fieldToBoost.put(Wiki13Indexer.CONTENT_ATTRIB, 0.85f);
		LOGGER.log(Level.INFO, "Running queries..");
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(
				queries, indexPath, new BM25Similarity(), fieldToBoost);
		return results;
	}

	protected static List<List<QueryResult>> filterResultsWithQueryThreshold(
			List<QueryResult> results, Map<String, Double> idPopMap) {
		List<List<QueryResult>> resultsList = new ArrayList<List<QueryResult>>();
		LOGGER.log(Level.INFO, "Caching results..");
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

	protected static double findThresholdPerDatabase(List<InexFile> inexFiles,
			double cutoffSize) {
		// TODO: what if cutoff == 0
		int lastItem = (int) Math.floor(cutoffSize * inexFiles.size() - 1);
		return inexFiles.get(lastItem).weight;
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
		for (int i = 0; i < result.getTopResults().size(); i++) {
			if (idPopMap.get(result.getTopResults().get(i)) >= cutoffWeight) {
				newTopResults.add(result.getTopResults().get(i));
				newTopResultTitles.add(result.getTopResultsTitle().get(i));
			}
		}
		newResult.setTopResults(newTopResults);
		newResult.setTopResultsTitle(newTopResultTitles);
		return newResult;
	}

	protected static void writeResultsListToFile(
			List<List<QueryResult>> resultsList, String resultDirectoryPath) {
		try (FileWriter p20Writer = new FileWriter("wiki_p20.csv");
				FileWriter mrrWriter = new FileWriter("wiki_mrr.csv");
				FileWriter rec200Writer = new FileWriter("wiki_recall200.csv");
				FileWriter recallWriter = new FileWriter("wiki_recall.csv");
				FileWriter logWriter = new FileWriter("log.csv")) {
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
