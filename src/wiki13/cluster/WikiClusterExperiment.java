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
	Option indexOption = new Option("index", false,
		"Flag to run indexing experiment");
	options.addOption(indexOption);
	Option queryOption = new Option("query", false,
		"Flag to run querying experiment");
	options.addOption(queryOption);
	Option totalExpNumberOption = new Option("total", true,
		"Total number of experiments");
	totalExpNumberOption.setRequired(true);
	options.addOption(totalExpNumberOption);
	Option expNumberOption = new Option("exp", true,
		"Number of experiment");
	expNumberOption.setRequired(true);
	options.addOption(expNumberOption);
	Option useMsnQueryLogOption = new Option("msn", false,
		"specifies the query log (msn/inex)");
	options.addOption(useMsnQueryLogOption);
	Option boostOption = new Option("boost", false, "Document boosting");
	options.addOption(boostOption);
	CommandLineParser clp = new DefaultParser();
	HelpFormatter formatter = new HelpFormatter();
	CommandLine cl;

	try {
	    cl = clp.parse(options, args);
	    int expNo = Integer.parseInt(cl.getOptionValue("exp"));
	    int totalExp = Integer.parseInt(cl.getOptionValue("total"));
	    float gamma = 0.15f;
	    String indexPath = WikiClusterPaths.INDEX_BASE + "wiki13_p"
		    + totalExp + "_w13" + "/part_" + expNo;
	    if (cl.hasOption("index")) {
		LOGGER.log(Level.INFO, "Building index..");
		WikiExperiment.buildGlobalIndex(expNo, totalExp,
			WikiClusterPaths.FILELIST_PATH, indexPath);
	    }
	    if (cl.hasOption("query")) {
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

		LOGGER.log(Level.INFO, "querying " + expNo + " at " + totalExp);
		long startTime = System.currentTimeMillis();
		List<QueryResult> results;
		if (cl.hasOption("boost")) {
		    results = WikiExperiment.runQueriesOnGlobalIndex(indexPath,
			    queries, gamma, true);
		} else {
		    results = WikiExperiment.runQueriesOnGlobalIndex(indexPath,
			    queries, gamma);
		}
		WikiExperiment.writeResultsToFile(results, "result/",
			expNo + ".csv");
		long endTime = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "logging.. ");
		Map<String, Double> idPopMap = PopularityUtils
			.loadIdPopularityMap(WikiClusterPaths.FILELIST_PATH);
		QueryResult.logResultsWithPopularity(results, idPopMap,
			"result/" + expNo + ".log", 20);
		LOGGER.log(Level.INFO, "Time spent for experiment " + expNo
			+ " is " + (endTime - startTime) / 1000 + " secs");
	    }
	} catch (org.apache.commons.cli.ParseException e) {
	    LOGGER.log(Level.INFO, e.getMessage());
	    formatter.printHelp("", options);
	    return;
	}
    }
}
