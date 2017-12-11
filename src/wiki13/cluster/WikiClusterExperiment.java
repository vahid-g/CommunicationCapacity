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
import wiki13.WikiFileIndexer;
import wiki13.querydifficulty.ClarityScore;
import wiki13.querydifficulty.LanguageModelScore;
import wiki13.querydifficulty.QueryDifficultyComputer;
import wiki13.querydifficulty.VarianceScore;
import wiki13.querydifficulty.VarianceScore.VarianceScoreMode;

public class WikiClusterExperiment {

    public static final Logger LOGGER = Logger
	    .getLogger(WikiClusterExperiment.class.getName());
    static final String INDEX_BASE = "/scratch/cluster-share/ghadakcv/data/index/";
    static final String FILELIST_PATH = "/scratch/cluster-share/ghadakcv/data/path_counts/wiki13_count13_text.csv";
    static final String FILELIST_PATH_COUNT09 = "/scratch/cluster-share/ghadakcv/data/path_counts/wiki13_count09_text.csv";
    static final String QUERYFILE_PATH = "/scratch/cluster-share/ghadakcv/data/queries/inex_ld/2013-ld-adhoc-topics.xml";
    static final String QREL_PATH = "/scratch/cluster-share/ghadakcv/data/queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
    static final String MSN_QUERY_QID = "/scratch/cluster-share/ghadakcv/data/queries/msn/query_qid.csv";
    static final String MSN_QID_QREL = "/scratch/cluster-share/ghadakcv/data/queries/msn/qid_qrel.csv";

    public static void main(String[] args) {

	Options options = new Options();
	Option indexOption = new Option("index", false,
		"Flag to run indexing experiment");
	options.addOption(indexOption);
	Option queryOption = new Option("query", false,
		"Flag to run querying experiment");
	options.addOption(queryOption);
	Option difficultyOption = new Option("diff", true,
		"Flag to run difficulty experiment");
	options.addOption(difficultyOption);
	Option totalExpNumberOption = new Option("total", true,
		"Total number of experiments");
	totalExpNumberOption.setRequired(true);
	options.addOption(totalExpNumberOption);
	Option expNumberOption = new Option("exp", true, "Number of experiment");
	expNumberOption.setRequired(true);
	options.addOption(expNumberOption);
	Option useMsnQueryLogOption = new Option("msn", false,
		"specifies the query log (msn/inex)");
	options.addOption(useMsnQueryLogOption);
	CommandLineParser clp = new DefaultParser();
	HelpFormatter formatter = new HelpFormatter();
	CommandLine cl;

	try {
	    cl = clp.parse(options, args);
	    int expNo = Integer.parseInt(cl.getOptionValue("exp"));
	    int totalExp = Integer.parseInt(cl.getOptionValue("total"));
	    String indexPath = INDEX_BASE + "wiki13_p" + totalExp + "_w13"
		    + "/part_" + expNo;
	    if (cl.hasOption("index")) {
		LOGGER.log(Level.INFO, "Building index..");
		WikiExperiment.buildGlobalIndex(expNo, totalExp, FILELIST_PATH,
			indexPath);
	    }
	    if (cl.hasOption("query")) {
		List<ExperimentQuery> queries;
		if (cl.hasOption("msn")) {
		    queries = QueryServices.loadMsnQueries(MSN_QUERY_QID,
			    MSN_QID_QREL);
		} else {
		    queries = QueryServices.loadInexQueries(QUERYFILE_PATH,
			    QREL_PATH, "title");
		}
		LOGGER.log(Level.INFO, "querying " + expNo + " at " + totalExp);
		long startTime = System.currentTimeMillis();
		List<QueryResult> results = WikiExperiment
			.runQueriesOnGlobalIndex(indexPath, queries, 0.15f);
		WikiExperiment.writeResultsToFile(results, "result/", expNo
			+ ".csv");
		long endTime = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "logging.. ");
		Map<String, Double> idPopMap = PopularityUtils
			.loadIdPopularityMap(FILELIST_PATH);
		QueryResult.logResultsWithPopularity(results, idPopMap,
			"result/" + expNo + ".log", 20);
		LOGGER.log(Level.INFO, "Time spent for experiment " + expNo
			+ " is " + (endTime - startTime) / 1000 + " secs");
	    }
	    if (cl.hasOption("diff")) {
		String difficultyMetric = cl.getOptionValue("diff");
		List<ExperimentQuery> queries;
		if (cl.hasOption("msn")) {
		    queries = QueryServices.loadMsnQueries(MSN_QUERY_QID,
			    MSN_QID_QREL);
		} else {
		    queries = QueryServices.loadInexQueries(QUERYFILE_PATH,
			    QREL_PATH, "title");
		}
		LOGGER.log(Level.INFO, "querylog size " + queries.size());
		QueryDifficultyComputer qdc;
		if (difficultyMetric.equals("scs")) {
		    qdc = new QueryDifficultyComputer(new ClarityScore());
		} else if (difficultyMetric.equals("maxvar")) {
		    qdc = new QueryDifficultyComputer(new VarianceScore(
			    VarianceScoreMode.MAX_VARIANCE));
		} else if (difficultyMetric.equals("avgvar")) {
		    qdc = new QueryDifficultyComputer(new VarianceScore(
			    VarianceScoreMode.AVERAGE_VARIANCE));
		} else if (difficultyMetric.equals("maxex")) {
		    qdc = new QueryDifficultyComputer(new VarianceScore(
			    VarianceScoreMode.MAX_EX));
		} else if (difficultyMetric.equals("avgex")) {
		    qdc = new QueryDifficultyComputer(new VarianceScore(
			    VarianceScoreMode.AVERAGE_EX));
		} else if (difficultyMetric.equals("lm")) {
		    qdc = new QueryDifficultyComputer(new LanguageModelScore());
		} else {
		    throw new org.apache.commons.cli.ParseException(
			    "Difficulty metric needs to be specified");
		}
		Map<String, Double> titleDifficulties = qdc
			.computeQueryDifficulty(indexPath, queries,
				WikiFileIndexer.TITLE_ATTRIB);
		Map<String, Double> contentDifficulties = qdc
			.computeQueryDifficulty(indexPath, queries,
				WikiFileIndexer.CONTENT_ATTRIB);
		WikiExperiment.writeMapToFile(titleDifficulties, "title_diff_"
			+ expNo + ".csv");
		WikiExperiment.writeMapToFile(contentDifficulties,
			"content_diff_" + expNo + ".csv");

	    }
	} catch (org.apache.commons.cli.ParseException e) {
	    LOGGER.log(Level.INFO, e.getMessage());
	    formatter.printHelp("", options);
	    return;
	}
    }
}
