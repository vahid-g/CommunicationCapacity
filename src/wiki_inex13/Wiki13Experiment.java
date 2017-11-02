package wiki_inex13;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.similarities.BM25Similarity;

import indexing.InexFile;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki_inex09.ClusterDirectoryInfo;

public class Wiki13Experiment {

	public static final Logger LOGGER = Logger.getLogger(Wiki13Experiment.class.getName());

	public static void main(String[] args) {

		long start_t = System.currentTimeMillis();

		int expNo = Integer.parseInt(args[0]);
		int totalExp = Integer.parseInt(args[1]);
		// float gamma = Float.parseFloat(args[2]);

		buildGlobalIndex(expNo, totalExp);
		// gridSearchExperiment(gamma);
		// expTextInex13(expNo, totalExp, gamma);
		// expTextMsn(expNo, totalExp);
		// runQueriesOnGlobalIndex(expNo, totalExp);

		LOGGER.log(Level.INFO, "Time spent for experiment " + expNo + " is "
				+ (System.currentTimeMillis() - start_t) / 60000 + " minutes");
	}

	static void gridSearchExperiment(float gamma) {
		// Note that the path count should be sorted!
		List<InexFile> pathCountList = InexFile.loadInexFileList(ClusterDirectoryInfo.PATH13_COUNT09);
		pathCountList = pathCountList.subList(0, pathCountList.size() / 10);
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "inex13_grid_"
				+ Float.toString(gamma).replace(".", "");
		LOGGER.log(Level.INFO, "Building index..");
		float gammas[] = new float[2];
		gammas[0] = gamma;
		gammas[1] = 1 - gamma;
		Wiki13Indexer.buildIndexOnText(pathCountList, indexName, gammas);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		// List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
		// ClusterDirectoryInfo.MSN_QUERY_QID_S,
		// ClusterDirectoryInfo.MSN_QID_QREL);
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(ClusterDirectoryInfo.INEX13_QUERY_FILE,
				ClusterDirectoryInfo.INEX13_QREL_FILE, "title");
		queries = queries.subList(0, queries.size() / 5);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<QueryResult> results = QueryServices.runQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(
				ClusterDirectoryInfo.RESULT_DIR + "inex13_grid_" + Float.toString(gamma).replace(".", "") + ".csv")) {
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

	public static void expTextMsn(int expNo, int totalExp) {
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "msn_index13_" + expNo;
		try {
			List<InexFile> pathCountList = InexFile.loadInexFileList(ClusterDirectoryInfo.PATH13_COUNT09);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0, (int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
			LOGGER.log(Level.INFO, "Smallest score: " + pathCountList.get(pathCountList.size() - 1).weight);
			LOGGER.log(Level.INFO, "Building index..");
			float gammas[] = { 0.9f, 0.1f };
			Wiki13Indexer.buildIndexOnText(pathCountList, indexName, gammas);
			LOGGER.log(Level.INFO, "Loading and running queries..");
			List<ExperimentQuery> queries = QueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID,
					ClusterDirectoryInfo.MSN_QID_QREL);
			LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
			List<QueryResult> results = QueryServices.runQueries(queries, indexName);
			LOGGER.log(Level.INFO, "Writing results..");
			try (FileWriter fw = new FileWriter(
					ClusterDirectoryInfo.RESULT_DIR + "msn13_" + totalExp + "_" + expNo + ".csv")) {
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

	public static void expTextInex13(int expNo, int totalExp, float gamma) {
		String indexPath = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "index13_" + expNo;
		try {
			List<InexFile> pathCountList = InexFile.loadInexFileList(ClusterDirectoryInfo.PATH13_COUNT13);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0, (int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
			LOGGER.log(Level.INFO, "Smallest score: " + pathCountList.get(pathCountList.size() - 1).weight);
			LOGGER.log(Level.INFO, "Building index..");
			float[] gammas = new float[2];
			gammas[0] = gamma;
			gammas[1] = 1 - gamma;
			Wiki13Indexer.buildIndexOnText(pathCountList, indexPath, gammas);
			LOGGER.log(Level.INFO, "Loading and running queries..");
			List<ExperimentQuery> queries = QueryServices.loadInexQueries(ClusterDirectoryInfo.INEX13_QUERY_FILE,
					ClusterDirectoryInfo.INEX13_QREL_FILE, "title");
			LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
			List<QueryResult> results = QueryServices.runQueries(queries, indexPath);
			LOGGER.log(Level.INFO, "Writing results..");
			String resultFileName = ClusterDirectoryInfo.RESULT_DIR + expNo + ".csv";
			String top10FileName = ClusterDirectoryInfo.RESULT_DIR + expNo + ".top";
			try (FileWriter fw = new FileWriter(resultFileName); FileWriter fw2 = new FileWriter(top10FileName)) {
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
	public static void buildGlobalIndex(int expNo, int totalExp) {
		try {
			List<InexFile> pathCountList = InexFile.loadInexFileList(ClusterDirectoryInfo.PATH13_COUNT09);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0, (int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
			LOGGER.log(Level.INFO, "Smallest score: " + pathCountList.get(pathCountList.size() - 1).weight);
			String indexPath = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "wiki13_p" + totalExp + "_w09_bm" + "/part_"
					+ expNo;
			File indexPathFile = new File(indexPath);
			if (!indexPathFile.exists()) {
				indexPathFile.mkdirs();
			}
			LOGGER.log(Level.INFO, "Building index at: " + indexPath);
			Wiki13Indexer.buildIndexOnText(pathCountList, indexPath, new BM25Similarity());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void runQueriesOnGlobalIndex(int expNo, int totalExp) {
		String indexPath = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "wiki13_p" + totalExp + "_w09_bm" + "/part_"
				+ expNo;
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(ClusterDirectoryInfo.INEX13_QUERY_FILE,
				ClusterDirectoryInfo.INEX13_QREL_FILE, "title");
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		fieldToBoost.put(Wiki13Indexer.TITLE_ATTRIB, 0.1f);
		fieldToBoost.put(Wiki13Indexer.CONTENT_ATTRIB, 0.9f);
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(queries, indexPath, new BM25Similarity(),
				fieldToBoost);
		LOGGER.log(Level.INFO, "Writing results..");
		String resultFileName = ClusterDirectoryInfo.RESULT_DIR + expNo + ".csv";
		try (FileWriter fw = new FileWriter(resultFileName)) {
			for (QueryResult iqr : results) {
				fw.write(iqr.resultString() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
