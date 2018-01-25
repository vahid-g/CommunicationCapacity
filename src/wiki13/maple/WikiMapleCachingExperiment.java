package wiki13.maple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import indexing.InexFile;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperiment;

public class WikiMapleCachingExperiment {

    private static final Logger LOGGER = Logger
	    .getLogger(WikiMapleCachingExperiment.class.getName());

    public static void main(String[] args) {
	Options options = new Options();
	Option indexOption = new Option("index", false, "run indexing mode");
	options.addOption(indexOption);
	Option queryOption = new Option("query", false, "run querying");
	options.addOption(queryOption);
	Option timingOption = new Option("timing", false,
		"run timig experiment");
	options.addOption(timingOption);
	Option useMsnOption = new Option("msn", false,
		"specifies the query log (msn/inex)");
	options.addOption(useMsnOption);
	Option partitionsOption = new Option("total", true,
		"number of partitions");
	options.addOption(partitionsOption);
	Option gammaOption = new Option("gamma", true,
		"the weight of title field");
	options.addOption(gammaOption);
	Option boostDocs = new Option("boost", false,
		"boost documents using their weights");
	options.addOption(boostDocs);
	CommandLineParser clp = new DefaultParser();
	HelpFormatter formatter = new HelpFormatter();
	CommandLine cl;
	try {
	    String indexDirPath = WikiMapleExperiment.DATA_PATH + "wiki_index/";
	    cl = clp.parse(options, args);
	    int partitionCount = Integer
		    .parseInt(cl.getOptionValue("total", "100"));
	    if (cl.hasOption("index")) {
		List<InexFile> files = InexFile
			.loadInexFileList(WikiMapleExperiment.FILELIST_PATH);
		for (double expNo = 1.0; expNo <= partitionCount; expNo++) {
		    double subsetFraction = expNo / partitionCount;
		    List<InexFile> subsetFiles = files.subList(0,
			    (int) (subsetFraction * files.size()));
		    WikiExperiment.buildGlobalIndex(subsetFiles,
			    indexDirPath + expNo);
		}
	    }
	    if (cl.hasOption("query") || cl.hasOption("timing")) {
		List<ExperimentQuery> queries;
		float gamma = Float
			.parseFloat(cl.getOptionValue("gamma", "0.15f"));
		if (cl.hasOption("msn")) {
		    queries = QueryServices.loadMsnQueries(
			    WikiMapleExperiment.MSN_QUERY_FILE_PATH,
			    WikiMapleExperiment.MSN_QREL_FILE_PATH);
		} else {
		    queries = QueryServices.loadInexQueries(
			    WikiMapleExperiment.QUERY_FILE_PATH,
			    WikiMapleExperiment.QREL_FILE_PATH, "title");
		}
		if (cl.hasOption("query")) {
		    for (int expNo = 1; expNo <= partitionCount; expNo++) {
			String indexPath = indexDirPath + expNo;
			long startTime = System.currentTimeMillis();
			List<QueryResult> results = WikiExperiment
				.runQueriesOnGlobalIndex(indexPath, queries,
					gamma);
			long spentTime = System.currentTimeMillis() - startTime;
			LOGGER.log(Level.INFO,
				"Time spent on querying " + queries.size()
					+ " queries is " + spentTime
					+ " seconds");
			WikiExperiment.writeResultsToFile(results, "result/",
				expNo + ".csv");
		    }
		} else if (cl.hasOption("timing")) {
		    long times[] = new long[partitionCount];
		    for (int i = 0; i < 10; i++) {
			for (int expNo = 1; expNo <= partitionCount; expNo++) {
			    String indexPath = indexDirPath + expNo;
			    long startTime = System.currentTimeMillis();
			    if (cl.hasOption("boost")) {
				WikiExperiment.runQueriesOnGlobalIndex(
					indexPath, queries, gamma, true);
			    } else {
				WikiExperiment.runQueriesOnGlobalIndex(
					indexPath, queries, gamma);
			    }
			    long spentTime = System.currentTimeMillis()
				    - startTime;
			    times[expNo - 1] += spentTime;
			    LOGGER.log(Level.INFO,
				    "Time spent on querying " + queries.size()
					    + " queries is " + spentTime
					    + " seconds");
			}
		    }
		    try (FileWriter fw = new FileWriter("time_results.csv")) {
			for (Long l : times) {
			    fw.write(l / 10.0 + "\n");
			}
		    } catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		    }
		}
	    }
	} catch (org.apache.commons.cli.ParseException e) {
	    LOGGER.log(Level.INFO, e.getMessage());
	    formatter.printHelp("", options);
	}
    }

}
