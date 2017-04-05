package wiki_inex09;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class Experiments {

	static final Logger LOGGER = Logger.getLogger(Experiments.class.getName());

	public static void main(String[] args) {
		// initializations
		File indexBaseDir = new File(ClusterDirectoryInfo.LOCAL_INDEX_BASE09);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(ClusterDirectoryInfo.RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();

		int expNo = Integer.parseInt(args[0]);
		int totalCount = Integer.parseInt(args[1]);
		long start_t = System.currentTimeMillis();
		expMsn(expNo, 0.9f, totalCount);
		long end_t = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "Time spent for experiment " + expNo + " is " + (end_t - start_t) / 60000 + " minutes");

		// float gamma = Float.parseFloat(args[0]);
		// gridSearchExperiment(gamma);

	}

	/**
	 * 
	 * This method loads Msn queries, and page visits of wikipedia. Then based
	 * on expNo, it selects a part of wikipedia with top page visits and builds
	 * an index on that.
	 * 
	 * It has the option to do score boosting with page counts (look below).
	 * 
	 * Then it runs msn queries on it and prints the results to a file. Also in
	 * the scoring, it multiplies weight of title by gamma and weight of body by
	 * (1 - gamma)
	 * 
	 * @param expNo
	 *            : this impcats the size of picked subset
	 * @param gamma
	 *            : this is the weight of title score vs. body score
	 * @param totalCount
	 *            TODO
	 */
	public static void expMsn(int expNo, float gamma, int totalCount) {
		Map<String, Integer> pathCountMap = loadPathCountMap(ClusterDirectoryInfo.PATH_COUNT_FILE09);
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountMap.size());
		LOGGER.log(Level.INFO, "Sorting files..");
		double doubleCount = (double) totalCount;
		int subsetSize = (int) (pathCountMap.size() * (expNo / doubleCount));
		Map<String, Integer> pathCountSorted = Utils.sortByValue(pathCountMap, subsetSize);

		String indexPath = ClusterDirectoryInfo.LOCAL_INDEX_BASE09 + "index09_" + expNo;
		LOGGER.log(Level.INFO, "Building index..");
		// WikiIndexer.buildIndexWless(pathCountSorted, indexPath, gamma);
		WikiIndexer.buildIndexBoosted(pathCountSorted, indexPath, gamma);

		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = QueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID_B,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<MsnQueryResult> results = QueryServices.runMsnQueries(queries, indexPath);
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(
				ClusterDirectoryInfo.RESULT_DIR + "msn09_" + totalCount + "_" + expNo + ".csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write(mqr.fullResult() + "\n");
			}
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexPath));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This experiment does the grid search to find the best params for
	 * weighting title vs. body in the retrieval scoring formula.
	 * 
	 * @param gamma
	 *            : weight of title
	 */
	public static void gridSearchExperiment(float gamma) {
		Map<String, Integer> pathCountMap = loadPathCountMap(ClusterDirectoryInfo.PATH_COUNT_FILE09);
		// Note! don't need to sort path_counts based on weight
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "inex09_grid_" + (gamma * 10);
		LOGGER.log(Level.INFO, "Building index..");
		WikiIndexer.buildIndexBoosted(pathCountMap, indexName, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = QueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID_S,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<MsnQueryResult> results = QueryServices.runMsnQueries(queries, indexName);
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
	 * Database size experiment using inex queires.
	 * 
	 * @param expNo
	 */
	public static void expInex(int expNo) {
		Map<String, Integer> pathCountMap = loadPathCountMap(ClusterDirectoryInfo.PATH_COUNT_FILE09);
		LOGGER.log(Level.INFO, "Sorting files..");
		int subsetSize = (int) (pathCountMap.size() * (expNo / 10.0));
		Map<String, Integer> pathCountSorted = Utils.sortByValue(pathCountMap, subsetSize);
		LOGGER.log(Level.INFO, "Building index..");
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE09 + "index_inex_" + expNo;
		WikiIndexer.buildIndexBoosted(pathCountSorted, indexName);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<InexQuery> queries = QueryServices.loadInexQueries(ClusterDirectoryInfo.INEX9_QUERY_FILE);
		queries.addAll(QueryServices.loadInexQueries(ClusterDirectoryInfo.INEX10_QUERY_FILE));
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<InexQueryResult> iqrList = QueryServices.runInexQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR + "inex_" + expNo + ".csv")) {
			for (InexQueryResult iqr : iqrList) {
				fw.write(iqr.toString());
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			LOGGER.log(Level.INFO, "cleanup..");
			try {
				File indexFile = new File(indexName);
				if (indexFile.exists())
					FileUtils.deleteDirectory(indexFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private static Map<String, Integer> loadPathCountMap(String pathCountFile) {
		LOGGER.log(Level.INFO, "Loading files list and counts");
		Map<String, Integer> pathCountMap = new HashMap<String, Integer>();
		try (BufferedReader br = new BufferedReader(new FileReader(pathCountFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (!line.contains(","))
					continue;
				String path = ClusterDirectoryInfo.CLUSTER_BASE + line.split(",")[0];
				Integer count = Integer.parseInt(line.split(",")[1].trim());
				pathCountMap.put(path, count);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountMap.size());
		return pathCountMap;
	}

}
