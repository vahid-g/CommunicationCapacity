package wiki13;

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

public class WikiSubsetQueryingExperiment {

	public static final Logger LOGGER = Logger.getLogger(WikiSubsetQueryingExperiment.class.getName());

	public static void main(String[] args) {
		Options options = new Options();
		Option totalPartitionCountOption = new Option("total", true, "Total number of experiments");
		totalPartitionCountOption.setRequired(true);
		options.addOption(totalPartitionCountOption);
		Option currentPartitionCountOption = new Option("exp", true, "Number of experiment");
		currentPartitionCountOption.setRequired(true);
		options.addOption(currentPartitionCountOption);
		Option useMsnQueryLogOption = new Option("queryset", true, "specifies the query log (msn/inex)");
		options.addOption(useMsnQueryLogOption);
		Option gammaOption = new Option("gamma", true, "Weight of title field in scoring");
		options.addOption(gammaOption);
		Option queryLogOption = new Option("log", false, "Generates verbose query logs");
		options.addOption(queryLogOption);
		Option boostOption = new Option("boost", false, "Uses document boosting based on access counts");
		options.addOption(boostOption);
		Option server = new Option("server", true, "Specifies maple/hpc");
		options.addOption(server);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;
		try {
			cl = clp.parse(options, args);
			final int currentPartition = Integer.parseInt(cl.getOptionValue("exp"));
			final int totalPartitionCount = Integer.parseInt(cl.getOptionValue("total"));
			final float gamma = Float.parseFloat(cl.getOptionValue("gamma", "0.15"));
			String indexFolder = "index/wiki13_p50_w09/";
			WikiFilesPaths paths = null;
			if (cl.getOptionValue("server").equals("maple")) {
				paths = WikiFilesPaths.getMaplePaths();
			} else if (cl.getOptionValue("server").equals("hpc")) {
				paths = WikiFilesPaths.getHpcPaths();
			}
			final boolean docBoost = cl.hasOption("boost");
			String indexPath = paths.getDataFolder() + indexFolder + currentPartition;
			List<ExperimentQuery> queries;
			if (cl.getOptionValue("queryset").equals("msn")) {
				queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(), paths.getMsnQrelFilePath());
			} else {
				queries = QueryServices.loadInexQueries(paths.getInexQueryFilePath(), paths.getInexQrelFilePath(),
						"title");
			}
			LOGGER.log(Level.INFO,
					"Submitting {0} queries to partition {1} at index {2} with gamma = {3} and docboost = {4}",
					new Object[] { queries.size(), currentPartition, totalPartitionCount, gamma, docBoost });
			long startTime = System.currentTimeMillis();
			List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, gamma,
					docBoost);
			long endTime = System.currentTimeMillis();
			LOGGER.log(Level.INFO, "Querying done in {0} seconds", (endTime - startTime) / 1000);
			WikiExperimentHelper.writeQueryResultsToFile(results, "result/", currentPartition + ".csv");
			if (cl.hasOption("log")) {
				LOGGER.log(Level.INFO, "logging.. ");
				Map<String, Double> idPopMap = PopularityUtils.loadIdPopularityMap(paths.getAccessCountsPath());
				QueryResult.logResultsWithPopularity(results, idPopMap, "result/" + currentPartition + ".log", 20);
				LOGGER.log(Level.INFO, "Logging done. ");
			}
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.ALL, e.getMessage());
			formatter.printHelp("", options);
			return;
		}
	}
}
