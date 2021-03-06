package wiki13;

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

import indexing.InexFile;
import popularity.PopularityUtils;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;

public class WikiFilteringExperiment {

	private static final Logger LOGGER = Logger.getLogger(WikiFilteringExperiment.class.getName());

	private static WikiFilesPaths PATHS = WikiFilesPaths.getMaplePaths();

	public static void main(String[] args) {
		Options options = new Options();
		Option filterOption = new Option("filter", false, "enables filtering mode");
		options.addOption(filterOption);
		Option cacheOption = new Option("cache", false, "enables caching mode");
		options.addOption(cacheOption);
		Option useMsnOption = new Option("msn", false, "specifies the query log (msn/inex)");
		options.addOption(useMsnOption);
		Option partitionsOption = new Option("total", true, "number of partitions");
		options.addOption(partitionsOption);
		Option iqOption = new Option("iq", false, "index and query");
		options.addOption(iqOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			int partitionCount = 100;
			cl = clp.parse(options, args);
			String flag = cl.getOptionValue("query");
			if (flag == null) {
				throw new org.apache.commons.cli.ParseException("-query needs an argument");
			}
			List<ExperimentQuery> queries;
			Map<String, Double> idPopMap;
			if (cl.hasOption("msn")) {
				queries = QueryServices.loadMsnQueries(PATHS.getMsnQueryFilePath(), PATHS.getMsnQrelFilePath());
				idPopMap = PopularityUtils.loadIdPopularityMap(PATHS.getAccessCounts09Path());
			} else {
				queries = QueryServices.loadInexQueries(PATHS.getInexQueryFilePath(), PATHS.getInexQrelFilePath(),
						"title");
				idPopMap = PopularityUtils.loadIdPopularityMap(PATHS.getAccessCountsPath());
			}
			List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(PATHS.getIndexBase() + "100",
					queries, 0.15f);
			if (cl.hasOption("cache")) {
				List<Double> thresholds = new ArrayList<Double>();
				List<InexFile> inexFiles = InexFile.loadInexFileList(PATHS.getAccessCountsPath());
				for (double i = 1; i <= partitionCount; i++) {
					int size = (int) Math.floor(inexFiles.size() * (i / (double) partitionCount) - 1);
					thresholds.add(inexFiles.get(size).weight);
				}
				LOGGER.log(Level.INFO, "Caching thresholds: {0}", thresholds);
				List<List<QueryResult>> resultsList = filterResultsWithSingleThreshold(results, idPopMap, thresholds);
				writeResultsListToFile(resultsList, "cache/");
			}
			if (cl.hasOption("filter")) {
				List<List<QueryResult>> resultsList = filterResultsWithQueryThreshold(results, idPopMap);
				writeResultsListToFile(resultsList, "filter/");
			}
		} catch (Exception e) {
			LOGGER.log(Level.INFO, e.getMessage());
			formatter.printHelp("", options);
		}
	}

	protected static List<List<QueryResult>> filterResultsWithSingleThreshold(List<QueryResult> results,
			Map<String, Double> idPopMap, List<Double> thresholds) {
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

	protected static List<List<QueryResult>> filterResultsWithQueryThreshold(List<QueryResult> results,
			Map<String, Double> idPopMap) {
		List<List<QueryResult>> resultsList = new ArrayList<List<QueryResult>>();
		LOGGER.log(Level.INFO, "Filtering results..");
		QueryResult newResult;
		for (double x = 0.02; x <= 1; x += 0.02) {
			List<QueryResult> newResults = new ArrayList<QueryResult>();
			for (QueryResult result : results) {
				double threshold = findThresholdPerQuery(result, idPopMap, x);
				newResult = filterQueryResult(result, idPopMap, threshold);
				newResults.add(newResult);
			}
			resultsList.add(newResults);
		}
		return resultsList;
	}

	protected static double findThresholdPerQuery(QueryResult result, Map<String, Double> idPopMap, double cutoffSize) {
		if (result.getTopDocuments().size() == 0) {
			LOGGER.log(Level.WARNING, "Query with no result {0}", result.query);
			return 0;
		}
		List<Double> pops = new ArrayList<Double>();
		for (QueryResult.TopDocument doc : result.getTopDocuments()) {
			String id = doc.id;
			pops.add(idPopMap.get(id));
		}
		Collections.sort(pops, Collections.reverseOrder());
		double cutoffWeight = pops.get((int) Math.max(0, Math.floor(cutoffSize * pops.size()) - 1));
		return cutoffWeight;
	}

	protected static QueryResult filterQueryResult(QueryResult result, Map<String, Double> idPopMap,
			double cutoffWeight) {
		QueryResult newResult = new QueryResult(result.query);
		if (result.getTopDocuments().size() < 2) {
			LOGGER.log(Level.WARNING, "query just has zero or one result");
			return newResult;
		}
		List<QueryResult.TopDocument> newTopDocuments = new ArrayList<QueryResult.TopDocument>();
		for (int i = 0; i < result.getTopDocuments().size(); i++) {
			String id = result.getTopDocuments().get(i).id;
			if (idPopMap.get(id) >= cutoffWeight) {
				newTopDocuments.add(result.getTopDocuments().get(i));

			}
		}
		newResult.setTopDocuments(newTopDocuments);
		return newResult;
	}
	
	private static void writeResultsListToFile(List<List<QueryResult>> resultsList, String resultDirectoryPath) {
		File resultsDir = new File(resultDirectoryPath);
		if (!resultsDir.exists())
			resultsDir.mkdirs();
		try (FileWriter p20Writer = new FileWriter(resultDirectoryPath + "wiki_p20.csv");
				FileWriter mrrWriter = new FileWriter(resultDirectoryPath + "wiki_mrr.csv");
				FileWriter rec200Writer = new FileWriter(resultDirectoryPath + "wiki_recall200.csv");
				FileWriter recallWriter = new FileWriter(resultDirectoryPath + "wiki_recall.csv")) {
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
}
