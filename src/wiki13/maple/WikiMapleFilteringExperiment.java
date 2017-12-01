package wiki13.maple;

import indexing.InexFile;

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

import popularity.PopularityUtils;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperiment;

public class WikiMapleFilteringExperiment {

    private static final Logger LOGGER = Logger
	    .getLogger(WikiMapleFilteringExperiment.class.getName());

    public static void main(String[] args) {
	Options options = new Options();
	Option filterOption = new Option("filter", false,
		"enables filtering mode");
	options.addOption(filterOption);
	Option cacheOption = new Option("cache", false, "enables caching mode");
	options.addOption(cacheOption);
	Option useMsnOption = new Option("msn", false,
		"specifies the query log (msn/inex)");
	options.addOption(useMsnOption);
	Option partitionsOption = new Option("total", true,
		"number of partitions");
	options.addOption(partitionsOption);
	Option iqOption = new Option("iq", false, "index and query");
	options.addOption(iqOption);
	CommandLineParser clp = new DefaultParser();
	HelpFormatter formatter = new HelpFormatter();
	CommandLine cl;

	try {
	    String indexPath = WikiMapleExperiment.DATA_PATH + "wiki_index/";
	    int partitionCount = 100;
	    cl = clp.parse(options, args);
	    String flag = cl.getOptionValue("query");
	    if (flag == null) {
		throw new org.apache.commons.cli.ParseException(
			"-query needs an argument");
	    }
	    List<ExperimentQuery> queries;
	    Map<String, Double> idPopMap;
	    if (cl.hasOption("msn")) {
		queries = QueryServices.loadMsnQueries(
			WikiMapleExperiment.MSN_QUERY_FILE_PATH,
			WikiMapleExperiment.MSN_QREL_FILE_PATH);
		idPopMap = PopularityUtils
			.loadIdPopularityMap(WikiMapleExperiment.FILELIST_COUNT09_PATH);
	    } else {
		queries = QueryServices.loadInexQueries(
			WikiMapleExperiment.QUERY_FILE_PATH,
			WikiMapleExperiment.QREL_FILE_PATH, "title");
		idPopMap = PopularityUtils
			.loadIdPopularityMap(WikiMapleExperiment.FILELIST_PATH);
	    }
	    List<QueryResult> results = WikiExperiment.runQueriesOnGlobalIndex(
		    indexPath, queries, 0.15f);
	    if (cl.hasOption("cache")) {
		List<Double> thresholds = new ArrayList<Double>();
		List<InexFile> inexFiles = InexFile
			.loadInexFileList(WikiMapleExperiment.FILELIST_PATH);
		for (double i = 1; i <= partitionCount; i++) {
		    int size = (int) Math.floor(inexFiles.size()
			    * (i / (double) partitionCount) - 1);
		    thresholds.add(inexFiles.get(size).weight);
		}
		LOGGER.log(Level.INFO, "Caching thresholds: {0}", thresholds);
		List<List<QueryResult>> resultsList = filterResultsWithSingleThreshold(
			results, idPopMap, thresholds);
		WikiMapleExperiment.writeResultsListToFile(resultsList,
			"cache/");
	    }
	    if (cl.hasOption("filter")) {
		List<List<QueryResult>> resultsList = filterResultsWithQueryThreshold(
			results, idPopMap);
		WikiMapleExperiment.writeResultsListToFile(resultsList,
			"filter/");
	    }
	} catch (Exception e) {
	    LOGGER.log(Level.INFO, e.getMessage());
	    formatter.printHelp("", options);
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

    protected static double findThresholdPerQuery(QueryResult result,
	    Map<String, Double> idPopMap, double cutoffSize) {
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
	double cutoffWeight = pops.get((int) Math.max(0,
		Math.floor(cutoffSize * pops.size()) - 1));
	return cutoffWeight;
    }

    protected static QueryResult filterQueryResult(QueryResult result,
	    Map<String, Double> idPopMap, double cutoffWeight) {
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
}
