package wiki_inex13;

import indexing.InexFileMetadata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki_inex09.ClusterDirectoryInfo;

public class ClusterExperiment13 {

	public static final Logger LOGGER = Logger.getLogger(ClusterExperiment13.class
			.getName());

	public static void main(String[] args) {

		// float gamma = Float.parseFloat(args[0]);
		// gridSearchExperiment(gamma);

		// int expNo = Integer.parseInt(args[0]);
		// int totalExpNo = Integer.parseInt(args[1]);
		// float gamma = 0.15f; // Float.parseFloat(args[2]);
		// long start_t = System.currentTimeMillis();
		// // expTextInex13(expNo, totalExpNo, gamma);
		// // expText(expNo, totalExpNo);
		// buildGlobalIndex(expNo, totalExpNo, gamma);
		// long end_t = System.currentTimeMillis();
		// LOGGER.log(Level.INFO, "Time spent for experiment " + expNo + " is "
		// + (end_t - start_t) / 60000 + " minutes");

		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				"data/queries/inex/all-topics.xml",
				"data/queries/inex/all-topics.qrels");
		System.out.println(queries.size());
	}

	static void gridSearchExperiment(float gamma) {
		// Note that the path count should be sorted!
		List<InexFileMetadata> pathCountList = InexFileMetadata.loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE13);
		pathCountList = pathCountList.subList(0, pathCountList.size() / 10);
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + pathCountList.size());
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13
				+ "inex13_grid_" + Float.toString(gamma).replace(".", "");
		LOGGER.log(Level.INFO, "Building index..");
		Wiki13Indexer.buildTextIndex(pathCountList, indexName, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		// List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
		// ClusterDirectoryInfo.MSN_QUERY_QID_S,
		// ClusterDirectoryInfo.MSN_QID_QREL);
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				ClusterDirectoryInfo.INEX13_QUERY_FILE,
				ClusterDirectoryInfo.INEX13_QREL_FILE);
		queries = queries.subList(0, queries.size() / 5);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<QueryResult> results = QueryServices
				.runQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR
				+ "inex13_grid_" + Float.toString(gamma).replace(".", "")
				+ ".csv")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.toString() + "\n");
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
	
	public static void expText(int expNo, int totalExp) {
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13
				+ "msn_index13_" + expNo;
		try {
			List<InexFileMetadata> pathCountList = InexFileMetadata.loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE13);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0,
					(int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: "
					+ pathCountList.get(0).weight);
			LOGGER.log(
					Level.INFO,
					"Smallest score: "
							+ pathCountList.get(pathCountList.size() - 1).weight);
			LOGGER.log(Level.INFO, "Building index..");
			Wiki13Indexer.buildTextIndex(pathCountList, indexName, 0.9f);
			// Wiki13Indexer.buildBoostedTextIndex(pathCountList, indexName,
			// 0.9f);
			LOGGER.log(Level.INFO, "Loading and running queries..");
			List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
					ClusterDirectoryInfo.MSN_QUERY_QID_B,
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

	public static void expTextInex13(int expNo, int totalExp, float gamma) {
		String indexPath = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "index13_"
				+ expNo;
		try {
			List<InexFileMetadata> pathCountList = InexFileMetadata.loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE13);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0,
					(int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: "
					+ pathCountList.get(0).weight);
			LOGGER.log(
					Level.INFO,
					"Smallest score: "
							+ pathCountList.get(pathCountList.size() - 1).weight);
			LOGGER.log(Level.INFO, "Building index..");
			Wiki13Indexer.buildTextIndex(pathCountList, indexPath, gamma);
			LOGGER.log(Level.INFO, "Loading and running queries..");
			HashMap<Integer, ExperimentQuery> queriesMap = QueryServices
					.buildQueries(ClusterDirectoryInfo.INEX13_QUERY_FILE,
							ClusterDirectoryInfo.INEX13_QREL_FILE);
			List<ExperimentQuery> queries = new ArrayList<ExperimentQuery>();
			queries.addAll(queriesMap.values());
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
					fw2.write(iqr.top10() + "\n");
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
	public static void buildGlobalIndex(int expNo, int totalExp, float gamma) {
		try {
			String indexPath = ClusterDirectoryInfo.GLOBAL_INDEX_BASE13
					+ totalExp + "_" + expNo + "_"
					+ Float.toString(gamma).replace(".", "");
			List<InexFileMetadata> pathCountList = InexFileMetadata.loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE13);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0,
					(int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: "
					+ pathCountList.get(0).weight);
			LOGGER.log(
					Level.INFO,
					"Smallest score: "
							+ pathCountList.get(pathCountList.size() - 1).weight);
			LOGGER.log(Level.INFO, "Building index..");
			Wiki13Indexer.buildTextIndex(pathCountList, indexPath, gamma);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
