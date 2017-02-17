package inex09;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class InexMsnExperiment {

	// static final String DATASET_PATH = "/scratch/data-sets/inex_09/";
	// static final String PATH_COUNT_FILE = "data/file_count.csv";
	// static final String QUERY_QID_FILE_PATH =
	// "data/queries/msn/query_qid.csv";
	// static final String QID_QREL_FILE_PATH = "data/queries/msn/qid_qrel.csv";
	// static final String INDEX_BASE = "data/index/index09/";
	// static final String RESULT_DIR = "data/result/";

	static final Logger LOGGER = Logger.getLogger(InexMsnExperiment.class.getName());

	public static void main(String[] args) {
		// initializations
		File indexBaseDir = new File(ClusterDirectoryInfo.LOCAL_INDEX_BASE09);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(ClusterDirectoryInfo.RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();

		// int expNo = Integer.parseInt(args[0]);
		// long start_t = System.currentTimeMillis();
		// exp(expNo);
		// long end_t = System.currentTimeMillis();
		// LOGGER.log(Level.INFO, "Time spent for experiment " + expNo + " is "
		// + (end_t - start_t) / 60000 + " minutes");

		float gamma = Float.parseFloat(args[0]);
		gridSearchExperiment(gamma);
	}

	static void gridSearchExperiment(float gamma) {
		LOGGER.log(Level.INFO, "Loading files list and counts");
		Map<String, Integer> pathCountMap = new HashMap<String, Integer>();
		try (BufferedReader br = new BufferedReader(new FileReader(ClusterDirectoryInfo.PATH_COUNT_FILE09))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (!line.contains(","))
					continue;
				String path = ClusterDirectoryInfo.CLUSTER_BASE + line.split(",")[0];
				Integer count = Integer.parseInt(line.split(",")[1].trim());
				Random rand = new Random();
				if (rand.nextDouble() < 0.2) { // samples 20%
					if (pathCountMap.containsKey(path))
						pathCountMap.put(path, count + pathCountMap.get(path));
					else
						pathCountMap.put(path, count);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountMap.size());
		// Note! don't need to sort path_counts based on weight
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "inex09_grid_" + (gamma * 10);
		LOGGER.log(Level.INFO, "Building index..");
		InexIndexer.buildIndex(pathCountMap, indexName, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID_S,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(
				ClusterDirectoryInfo.RESULT_DIR + "inex09_grid_" + Float.toString(gamma).replace(",", "") + ".csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write(mqr.toString());
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

	/**
	 * 
	 * This method loads Msn queries, and page visits of wikipedia. Then based
	 * on expNo, it selects a part of wikipedia with top page visits and builds
	 * an index on that. Then it runs msn queries on it and prints the results
	 * to a file.
	 * 
	 * @param expNo
	 */
	public static void exp(int expNo) {
		LOGGER.log(Level.INFO, "Loading files list and counts");
		Map<String, Integer> pathCountMap = new HashMap<String, Integer>();
		try (BufferedReader br = new BufferedReader(new FileReader(ClusterDirectoryInfo.PATH_COUNT_FILE09))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (!line.contains(","))
					continue;
				String path = ClusterDirectoryInfo.CLUSTER_BASE + line.split(",")[0];
				Integer count = Integer.parseInt(line.split(",")[1].trim());
				if (pathCountMap.containsKey(path))
					pathCountMap.put(path, count + pathCountMap.get(path));
				else
					pathCountMap.put(path, count);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountMap.size());
		LOGGER.log(Level.INFO, "Sorting files..");
		int subsetSize = (int) (pathCountMap.size() * (expNo / 10.0));
		Map<String, Integer> pathCountSorted = Utils.sortByValue(pathCountMap, subsetSize);

		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE09 + "index_inex_" + expNo;
		LOGGER.log(Level.INFO, "Building index..");
		InexIndexer.buildIndex(pathCountSorted, indexName);

		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR + "msn09_" + expNo + ".csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write("\"" + mqr.msnQuery.text.replace(",", "") + "\", " + mqr.precisionAtK(3) + ", " + mqr.mrr()
						+ mqr.top3[0] + ", " + mqr.top3[1] + ", " + mqr.top3[2] + "\n");
			}
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This runs all msn queries on all wikipedia without considering weights.
	 */
	public static void exp0() {
		List<String> allFiles = Utils.listFilesForFolder(new File(ClusterDirectoryInfo.DATASET09_PATH));
		InexIndexer.buildIndex(allFiles.toArray(new String[0]), ClusterDirectoryInfo.LOCAL_INDEX_BASE09);
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID,
				ClusterDirectoryInfo.MSN_QID_QREL);
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries,
				ClusterDirectoryInfo.LOCAL_INDEX_BASE09);
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR + "inex.csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write(mqr.msnQuery.text + ", " + mqr.precisionAtK(3) + ", " + mqr.mrr() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
