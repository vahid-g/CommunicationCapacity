package wiki13;

import indexing.InexFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.similarities.BM25Similarity;

import popularity.PopularityUtils;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;

public class Wiki13Experiment {

	public static final Logger LOGGER = Logger.getLogger(Wiki13Experiment.class
			.getName());
	private static final String INDEX_BASE = "/scratch/cluster-share/ghadakcv/data/index/";
	private static final String FILELIST_PATH = "/scratch/cluster-share/ghadakcv/data/path_counts/wiki13_count13_text.csv";
	private static final String FILELIST_PATH_COUNT09 = "/scratch/cluster-share/ghadakcv/data/path_counts/wiki13_count09_text.csv";
	private static final String QUERYFILE_PATH = "/scratch/cluster-share/ghadakcv/data/queries/inex_ld/2013-ld-adhoc-topics.xml";
	private static final String QREL_PATH = "/scratch/cluster-share/ghadakcv/data/queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
	private static final String MSN_QUERY_QID = "/scratch/cluster-share/ghadakcv/data/msn/query_qid.csv";
	private static final String MSN_QID_QREL = "/scratch/cluster-share/ghadakcv/data/msn/qid_qrel.csv";

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
				buildGlobalIndex(expNo, totalExp, FILELIST_PATH, indexPath);
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
				List<QueryResult> results = runQueriesOnGlobalIndex(indexPath,
						queries, 0.1f);
				writeResultsToFile(results, "result/" + expNo + ".csv");
				long endTime = System.currentTimeMillis();
				LOGGER.log(Level.INFO, "logging.. ");
				Map<String, Double> idPopMap = PopularityUtils
						.loadIdPopularityMap(FILELIST_PATH);
				QueryResult.logResultsWithPopularity(results, idPopMap,
						"result/" + expNo + ".log", 20);
				LOGGER.log(Level.INFO, "Time spent for experiment " + expNo
						+ " is " + (endTime - startTime) / 60000 + " minutes");
			}
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.INFO, e.getMessage());
			formatter.printHelp("", options);
			return;
		}
	}

	static void gridSearchExperiment(float gamma, String queryFilePath,
			String qrelPath) {
		// Note that the path count should be sorted!
		List<InexFile> pathCountList = InexFile
				.loadInexFileList(FILELIST_PATH_COUNT09);
		pathCountList = pathCountList.subList(0, pathCountList.size() / 10);
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + pathCountList.size());
		String indexName = "/scratch/ghadakcv/index/" + "inex13_grid_"
				+ Float.toString(gamma).replace(".", "");
		LOGGER.log(Level.INFO, "Building index..");
		float gammas[] = new float[2];
		gammas[0] = gamma;
		gammas[1] = 1 - gamma;
		Wiki13Indexer.buildIndexOnText(pathCountList, indexName, gammas);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				queryFilePath, qrelPath, "title");
		queries = queries.subList(0, queries.size() / 5);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<QueryResult> results = QueryServices
				.runQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter("inex13_grid_"
				+ Float.toString(gamma).replace(".", "") + ".csv")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.resultString() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void expTextMsn(int expNo, int totalExp) {
		String indexName = "/scratch/ghadakcv/index/" + "msn_index13_" + expNo;
		try {
			List<InexFile> pathCountList = InexFile
					.loadInexFileList(FILELIST_PATH_COUNT09);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0,
					(int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
			LOGGER.log(
					Level.INFO,
					"Smallest score: "
							+ pathCountList.get(pathCountList.size() - 1).weight);
			LOGGER.log(Level.INFO, "Building index..");
			float gammas[] = {0.9f, 0.1f};
			Wiki13Indexer.buildIndexOnText(pathCountList, indexName, gammas);
			LOGGER.log(Level.INFO, "Loading and running queries..");
			List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
					MSN_QUERY_QID, MSN_QID_QREL);
			LOGGER.log(Level.INFO,
					"Number of loaded queries: " + queries.size());
			List<QueryResult> results = QueryServices.runQueries(queries,
					indexName);
			LOGGER.log(Level.INFO, "Writing results..");
			try (FileWriter fw = new FileWriter("msn13_" + totalExp + "_"
					+ expNo + ".csv")) {
				for (QueryResult mqr : results) {
					fw.write(mqr.toString() + "\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			try {
				LOGGER.log(Level.INFO, "cleanup..");
				FileUtils.deleteDirectory(new File(indexName));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	static void expTextInex(int expNo, int totalExp, float gamma,
			String queryFilePath, String qrelPath, String filelistPath) {
		String indexPath = "/scratch/ghadakcv/index/" + "index13_" + expNo;
		try {
			List<InexFile> pathCountList = InexFile
					.loadInexFileList(filelistPath);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0,
					(int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
			LOGGER.log(
					Level.INFO,
					"Smallest score: "
							+ pathCountList.get(pathCountList.size() - 1).weight);
			LOGGER.log(Level.INFO, "Building index..");
			float[] gammas = new float[2];
			gammas[0] = gamma;
			gammas[1] = 1 - gamma;
			Wiki13Indexer.buildIndexOnText(pathCountList, indexPath, gammas);
			LOGGER.log(Level.INFO, "Loading and running queries..");
			List<ExperimentQuery> queries = QueryServices.loadInexQueries(
					queryFilePath, qrelPath, "title");
			LOGGER.log(Level.INFO,
					"Number of loaded queries: " + queries.size());
			List<QueryResult> results = QueryServices.runQueries(queries,
					indexPath);
			LOGGER.log(Level.INFO, "Writing results..");
			String resultFileName = expNo + ".csv";
			try (FileWriter fw = new FileWriter(resultFileName)) {
				for (QueryResult iqr : results) {
					fw.write(iqr.toString() + "\n");
				}
				LOGGER.log(Level.INFO, "cleanup..");
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			try {
				FileUtils.deleteDirectory(new File(indexPath));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// builds the index on cluster-share
	static void buildGlobalIndex(int expNo, int totalExp,
			String filelistPopularityPath, String indexPath) {
		try {
			List<InexFile> pathCountList = InexFile
					.loadInexFileList(filelistPopularityPath);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0,
					(int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
			LOGGER.log(
					Level.INFO,
					"Smallest score: "
							+ pathCountList.get(pathCountList.size() - 1).weight);
			File indexPathFile = new File(indexPath);
			if (!indexPathFile.exists()) {
				indexPathFile.mkdirs();
			}
			LOGGER.log(Level.INFO, "Building index at: " + indexPath);
			Wiki13Indexer.buildIndexOnText(pathCountList, indexPath,
					new BM25Similarity());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static List<QueryResult> runQueriesOnGlobalIndex(String indexPath, List<ExperimentQuery> queries, float gamma) {
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		fieldToBoost.put(Wiki13Indexer.TITLE_ATTRIB, gamma);
		fieldToBoost.put(Wiki13Indexer.CONTENT_ATTRIB, 1 - gamma);
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(
				queries, indexPath, new BM25Similarity(), fieldToBoost, false);
		return results;
	}

	static void writeResultsToFile(List<QueryResult> results,
			String resultFileName) {
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(resultFileName)) {
			for (QueryResult iqr : results) {
				fw.write(iqr.resultString() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
