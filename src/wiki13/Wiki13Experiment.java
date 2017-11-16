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
import wiki09.ClusterDirectoryInfo;

public class Wiki13Experiment {

	public static final Logger LOGGER = Logger.getLogger(Wiki13Experiment.class
			.getName());
	private static final String INDEX_BASE = "/scratch/cluster-share/ghadakcv/data/index/";
	public static final String FILELIST_PATH = "/scratch/cluster-share/ghadakcv/data/path_counts/wiki13_count13_text.csv";

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
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			cl = clp.parse(options, args);
			int expNo = Integer.parseInt(cl.getOptionValue("e"));
			int totalExp = Integer.parseInt(cl.getOptionValue("t"));
			String indexPath = INDEX_BASE + "wiki13_p" + totalExp + "_w13"
					+ "/part_" + expNo;
			long start_t = System.currentTimeMillis();
			if (cl.hasOption("index")) {
				LOGGER.log(Level.INFO, "Building index..");
				buildGlobalIndex(expNo, totalExp, FILELIST_PATH, indexPath);
			}
			if (cl.hasOption("query")) {
				LOGGER.log(Level.INFO, "querying " + expNo + " at " + totalExp);
				List<QueryResult> results = runQueriesOnGlobalIndex(expNo,
						totalExp, indexPath);
				writeResultsToFile(results, "result/" + expNo + ".csv");
				Map<String, Double> idPopMap = PopularityUtils
						.loadIdPopularityMap(FILELIST_PATH);
				QueryResult.logResultsWithPopularity(results, idPopMap, expNo
						+ ".log");
			}
			LOGGER.log(Level.INFO, "Time spent for experiment " + expNo
					+ " is " + (System.currentTimeMillis() - start_t) / 60000
					+ " minutes");
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.INFO, e.getMessage());
			formatter.printHelp("", options);
			return;
		}
	}
	static void gridSearchExperiment(float gamma) {
		// Note that the path count should be sorted!
		List<InexFile> pathCountList = InexFile
				.loadInexFileList(ClusterDirectoryInfo.PATH13_COUNT09);
		pathCountList = pathCountList.subList(0, pathCountList.size() / 10);
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + pathCountList.size());
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13
				+ "inex13_grid_" + Float.toString(gamma).replace(".", "");
		LOGGER.log(Level.INFO, "Building index..");
		float gammas[] = new float[2];
		gammas[0] = gamma;
		gammas[1] = 1 - gamma;
		Wiki13Indexer.buildIndexOnText(pathCountList, indexName, gammas);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		// List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
		// ClusterDirectoryInfo.MSN_QUERY_QID_S,
		// ClusterDirectoryInfo.MSN_QID_QREL);
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				ClusterDirectoryInfo.INEX13_QUERY_FILE,
				ClusterDirectoryInfo.INEX13_QREL_FILE, "title");
		queries = queries.subList(0, queries.size() / 5);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<QueryResult> results = QueryServices
				.runQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR
				+ "inex13_grid_" + Float.toString(gamma).replace(".", "")
				+ ".csv")) {
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
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13
				+ "msn_index13_" + expNo;
		try {
			List<InexFile> pathCountList = InexFile
					.loadInexFileList(ClusterDirectoryInfo.PATH13_COUNT09);
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
					ClusterDirectoryInfo.MSN_QUERY_QID,
					ClusterDirectoryInfo.MSN_QID_QREL);
			LOGGER.log(Level.INFO,
					"Number of loaded queries: " + queries.size());
			List<QueryResult> results = QueryServices.runQueries(queries,
					indexName);
			LOGGER.log(Level.INFO, "Writing results..");
			try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR
					+ "msn13_" + totalExp + "_" + expNo + ".csv")) {
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

	static void expTextInex13(int expNo, int totalExp, float gamma) {
		String indexPath = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "index13_"
				+ expNo;
		try {
			List<InexFile> pathCountList = InexFile
					.loadInexFileList(ClusterDirectoryInfo.PATH13_COUNT13);
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
					ClusterDirectoryInfo.INEX13_QUERY_FILE,
					ClusterDirectoryInfo.INEX13_QREL_FILE, "title");
			LOGGER.log(Level.INFO,
					"Number of loaded queries: " + queries.size());
			List<QueryResult> results = QueryServices.runQueries(queries,
					indexPath);
			LOGGER.log(Level.INFO, "Writing results..");
			String resultFileName = ClusterDirectoryInfo.RESULT_DIR + expNo
					+ ".csv";
			String top10FileName = ClusterDirectoryInfo.RESULT_DIR + expNo
					+ ".top";
			try (FileWriter fw = new FileWriter(resultFileName);
					FileWriter fw2 = new FileWriter(top10FileName)) {
				for (QueryResult iqr : results) {
					fw.write(iqr.toString() + "\n");
					fw2.write(iqr.logTopResults() + "\n");
				}
				LOGGER.log(Level.INFO, "cleanup..");

				// to outer layer
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

	static List<QueryResult> runQueriesOnGlobalIndex(int expNo, int totalExp,
			String indexPath) {
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				ClusterDirectoryInfo.INEX13_QUERY_FILE,
				ClusterDirectoryInfo.INEX13_QREL_FILE, "title");
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		fieldToBoost.put(Wiki13Indexer.TITLE_ATTRIB, 0.1f);
		fieldToBoost.put(Wiki13Indexer.CONTENT_ATTRIB, 0.9f);
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(
				queries, indexPath, new BM25Similarity(), fieldToBoost);
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