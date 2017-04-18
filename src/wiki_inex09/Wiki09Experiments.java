package wiki_inex09;

import indexing.InexFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;

public class Wiki09Experiments {

	static final Logger LOGGER = Logger.getLogger(Wiki09Experiments.class.getName());

	public static void main(String[] args) {

		int expNo = Integer.parseInt(args[0]);
		int totalCount = Integer.parseInt(args[1]);
		long start_t = System.currentTimeMillis();
		expMsn(expNo, 0.9f, totalCount);
		long end_t = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "Time spent for experiment " + expNo + " is "
				+ (end_t - start_t) / 60000 + " minutes");

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
	 */
	public static void expMsn(int expNo, float gamma, int totalCount) {
		// file should be sorted
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE09);
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + fileList.size());
		LOGGER.log(Level.INFO, "Sorting files..");
		LOGGER.log(Level.INFO, "Building index..");
		String indexPath = ClusterDirectoryInfo.LOCAL_INDEX_BASE09 + "index09_"
				+ expNo;
		double doubleCount = (double) totalCount;
		int subsetSize = (int) (fileList.size() * (expNo / doubleCount));
		new Wiki09Indexer().buildIndex(fileList.subList(0, subsetSize),
				indexPath, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
				ClusterDirectoryInfo.MSN_QUERY_QID_B,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<QueryResult> results = QueryServices
				.runQueries(queries, indexPath);
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR
				+ "msn09_" + totalCount + "_" + expNo + ".csv")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.toString() + "\n");
			}
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexPath));
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
		// list should be sorted
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE09);
		LOGGER.log(Level.INFO, "Sorting files..");
		int subsetSize = (int) (fileList.size() * (expNo / 10.0));
		LOGGER.log(Level.INFO, "Building index..");
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE09
				+ "index_inex_" + expNo;
		new Wiki09Indexer().buildIndex(fileList.subList(0, subsetSize),
				indexName, 0.5f);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				ClusterDirectoryInfo.INEX9_QUERY_FILE,
				ClusterDirectoryInfo.INEX9_QUERY_FILE);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<QueryResult> iqrList = QueryServices
				.runQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR
				+ "inex_" + expNo + ".csv")) {
			for (QueryResult iqr : iqrList) {
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

	/**
	 * This experiment does the grid search to find the best params for
	 * weighting title vs. body in the retrieval scoring formula.
	 * 
	 * @param gamma
	 *            : weight of title
	 */
	public static void gridSearchExperiment(float gamma) {
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE09);
		// Note! don't need to sort path_counts based on weight
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13
				+ "inex09_grid_" + (gamma * 10);
		LOGGER.log(Level.INFO, "Building index..");
		new Wiki09Indexer().buildIndex(fileList, indexName, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
				ClusterDirectoryInfo.MSN_QUERY_QID_S,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<QueryResult> results = QueryServices
				.runQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR
				+ "inex09_grid_" + Float.toString(gamma).replace(",", "")
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

}
