package wiki13.maple;

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
	Option useMsnOption = new Option("msn", false,
		"specifies the query log (msn/inex)");
	options.addOption(useMsnOption);
	Option partitionsOption = new Option("total", true,
		"number of partitions");
	options.addOption(partitionsOption);
	CommandLineParser clp = new DefaultParser();
	HelpFormatter formatter = new HelpFormatter();
	CommandLine cl;
	try {
	    String indexDirPath = WikiMapleExperiment.DATA_PATH + "wiki_index/";
	    cl = clp.parse(options, args);
	    int partitionCount = Integer.parseInt(cl.getOptionValue("total",
		    "100"));
	    if (cl.hasOption("index")) {
		List<InexFile> files = InexFile
			.loadInexFileList(WikiMapleExperiment.FILELIST_PATH);
		for (double expNo = 1.0; expNo <= partitionCount; expNo++) {
		    double subsetFraction = expNo / partitionCount;
		    List<InexFile> subsetFiles = files.subList(0,
			    (int) (subsetFraction * files.size()));
		    WikiExperiment.buildGlobalIndex(subsetFiles, indexDirPath
			    + expNo);
		}
	    }
	    if (cl.hasOption("query")) {
		List<ExperimentQuery> queries;
		if (cl.hasOption("msn")) {
		    queries = QueryServices.loadMsnQueries(
			    WikiMapleExperiment.MSN_QUERY_FILE_PATH,
			    WikiMapleExperiment.MSN_QREL_FILE_PATH);
		} else {
		    queries = QueryServices.loadInexQueries(
			    WikiMapleExperiment.QUERY_FILE_PATH,
			    WikiMapleExperiment.QREL_FILE_PATH, "title");
		}
		for (int expNo = 1; expNo <= partitionCount; expNo++) {
		    String indexPath = indexDirPath + expNo;
		    List<QueryResult> results = WikiExperiment
			    .runQueriesOnGlobalIndex(indexPath, queries, 0.15f);
		    WikiExperiment.writeResultsToFile(results, "result/", expNo
			    + ".csv");
		}
	    }
	} catch (org.apache.commons.cli.ParseException e) {
	    LOGGER.log(Level.INFO, e.getMessage());
	    formatter.printHelp("", options);
	}
    }

}
