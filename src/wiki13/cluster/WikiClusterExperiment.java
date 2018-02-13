package wiki13.cluster;

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

public class WikiClusterExperiment {

    public static final Logger LOGGER = Logger
	    .getLogger(WikiClusterExperiment.class.getName());

    public static void main(String[] args) {
	Options options = new Options();
	Option totalPartitionCountOption = new Option("total", true,
		"Total number of experiments");
	totalPartitionCountOption.setRequired(true);
	options.addOption(totalPartitionCountOption);
	Option currentPartitionCountOption = new Option("exp", true,
		"Number of experiment");
	currentPartitionCountOption.setRequired(true);
	options.addOption(currentPartitionCountOption);
	Option useMsnQueryLogOption = new Option("msn", false,
		"specifies the query log (msn/inex)");
	options.addOption(useMsnQueryLogOption);
	Option gammaOption = new Option("gamma", true,
		"Weight of Title in scoring");
	options.addOption(gammaOption);
	Option queryLogOption = new Option("log", false,
		"Generates verbose query logs");
	options.addOption(queryLogOption);
	Option boostOption = new Option("boost", false, "Document boosting");
	options.addOption(boostOption);
	CommandLineParser clp = new DefaultParser();
	HelpFormatter formatter = new HelpFormatter();
	CommandLine cl;
	try {
	    cl = clp.parse(options, args);
	    final int currentPartition = Integer
		    .parseInt(cl.getOptionValue("exp"));
	    final int totalPartitionCount = Integer
		    .parseInt(cl.getOptionValue("total"));
	    final float gamma = Float
		    .parseFloat(cl.getOptionValue("gamma", "0.15"));
	    final boolean docBoost = cl.hasOption("boost");
	    String indexPath = WikiClusterPaths.INDEX_BASE + "wiki13_p"
		    + totalPartitionCount + "_w13" + "/part_"
		    + currentPartition;
	    List<ExperimentQuery> queries;
	    if (cl.hasOption("msn")) {
		queries = QueryServices.loadMsnQueries(
			WikiClusterPaths.MSN_QUERY_QID,
			WikiClusterPaths.MSN_QID_QREL);
	    } else {
		queries = QueryServices.loadInexQueries(
			WikiClusterPaths.QUERYFILE_PATH,
			WikiClusterPaths.QREL_PATH, "title");
	    }
	    LOGGER.log(Level.INFO,
		    "Submitting {0} queries to partition {1} at index {2} with gamma = {3} and docboost = {4}",
		    new Object[] { queries.size(), currentPartition,
			    totalPartitionCount, gamma, docBoost });
	    long startTime = System.currentTimeMillis();
	    List<QueryResult> results = WikiExperiment.runQueriesOnGlobalIndex(
		    indexPath, queries, gamma, docBoost);
	    long endTime = System.currentTimeMillis();
	    LOGGER.log(Level.INFO, "Querying done in {0} seconds",
		    (endTime - startTime) / 1000);
	    WikiExperiment.writeQueryResultsToFile(results, "result/",
		    currentPartition + ".csv");
	    if (cl.hasOption("log")) {
		LOGGER.log(Level.INFO, "logging.. ");
		Map<String, Double> idPopMap = PopularityUtils
			.loadIdPopularityMap(WikiClusterPaths.FILELIST_PATH);
		QueryResult.logResultsWithPopularity(results, idPopMap,
			"result/" + currentPartition + ".log", 20);
		LOGGER.log(Level.INFO, "Logging done. ");
	    }
	} catch (org.apache.commons.cli.ParseException e) {
	    LOGGER.log(Level.INFO, e.getMessage());
	    formatter.printHelp("", options);
	    return;
	}
    }
}
