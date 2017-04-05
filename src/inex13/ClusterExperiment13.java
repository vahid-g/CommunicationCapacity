package inex13;

import inex09.ClusterDirectoryInfo;
import inex09.InexQuery;
import inex09.InexQueryResult;
import inex09.QueryServices;
import inex09.MsnQuery;
import inex09.MsnQueryResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class ClusterExperiment13 {

	static final Logger LOGGER = Logger.getLogger(ClusterExperiment13.class
			.getName());

	public static void main(String[] args) {

		// float gamma = Float.parseFloat(args[0]);
		// gridSearchExperiment(gamma);

		int expNo = Integer.parseInt(args[0]);
		int totalExpNo = Integer.parseInt(args[1]);
		float gamma = Float.parseFloat(args[2]);
		long start_t = System.currentTimeMillis();
		expTextInex13(expNo, totalExpNo, gamma);
		long end_t = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "Time spent for experiment " + expNo + " is "
				+ (end_t - start_t) / 60000 + " minutes");

	}

	static void gridSearchExperiment(float gamma) {
		List<PathCountTitle> pathCountList = loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE13);
		// TODO sort?
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + pathCountList.size());
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13
				+ "inex13_grid_" + (gamma * 10);
		LOGGER.log(Level.INFO, "Building index..");
		Wiki13Indexer.buildBoostedIndex(pathCountList, indexName, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = QueryServices.loadMsnQueries(
				ClusterDirectoryInfo.MSN_QUERY_QID_S,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<MsnQueryResult> results = QueryServices.runMsnQueries(queries,
				indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR
				+ "inex13_grid_" + (gamma * 10) + ".csv")) {
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

	public static void expText(int expNo, int totalExp) {
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "index13_"
				+ expNo;
		try {
			List<PathCountTitle> pathCountList = loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE13);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0,
					(int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: "
					+ pathCountList.get(0).visitCount);
			LOGGER.log(
					Level.INFO,
					"Smallest score: "
							+ pathCountList.get(pathCountList.size() - 1).visitCount);
			LOGGER.log(Level.INFO, "Building index..");
			// InexIndexer.buildTextIndex(pathCountList, indexName, 0.9f);
			Wiki13Indexer.buildBoostedTextIndex(pathCountList, indexName, 0.9f);
			LOGGER.log(Level.INFO, "Loading and running queries..");
			List<MsnQuery> queries = QueryServices.loadMsnQueries(
					ClusterDirectoryInfo.MSN_QUERY_QID_B,
					ClusterDirectoryInfo.MSN_QID_QREL);
			LOGGER.log(Level.INFO,
					"Number of loaded queries: " + queries.size());
			List<MsnQueryResult> results = QueryServices.runMsnQueries(queries,
					indexName);
			LOGGER.log(Level.INFO, "Writing results..");
			try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR
					+ "msn13_" + totalExp + "_" + expNo + ".csv")) {
				for (MsnQueryResult mqr : results) {
					fw.write(mqr.fullResult() + "\n");
				}
				LOGGER.log(Level.INFO, "cleanup..");

			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			try {
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
			List<PathCountTitle> pathCountList = loadFilePathCountTitle(ClusterDirectoryInfo.PATH_COUNT_FILE13);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0,
					(int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: "
					+ pathCountList.get(0).visitCount);
			LOGGER.log(
					Level.INFO,
					"Smallest score: "
							+ pathCountList.get(pathCountList.size() - 1).visitCount);
			LOGGER.log(Level.INFO, "Building index..");

			Wiki13Indexer.buildTextIndex(pathCountList, indexPath, gamma);
			LOGGER.log(Level.INFO, "Loading and running queries..");
			String QUERY_FILE = ClusterDirectoryInfo.CLUSTER_BASE
					+ "data/inex_ld/2013-ld-adhoc-topics.xml";
			String QREL_FILE = ClusterDirectoryInfo.CLUSTER_BASE
					+ "data/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
			HashMap<Integer, InexQuery> queriesMap = QueryParser.buildQueries(
					QUERY_FILE, QREL_FILE);
			List<InexQuery> queries = new ArrayList<InexQuery>();
			queries.addAll(queriesMap.values());
			LOGGER.log(Level.INFO,
					"Number of loaded queries: " + queries.size());
			List<InexQueryResult> results = QueryServices.runInexQueries(
					queries, indexPath);
			LOGGER.log(Level.INFO, "Writing results..");
			String resultFileName = ClusterDirectoryInfo.RESULT_DIR + expNo
					+ "_g" + Float.toString(gamma).replace(".", "") + "_"
					+ totalExp + "_" + expNo + ".csv";
			String top10FileName = ClusterDirectoryInfo.RESULT_DIR + expNo
					+ "_g" + Float.toString(gamma).replace(".", "") + "_"
					+ totalExp + "_" + expNo + "_top10.csv";
			try (FileWriter fw = new FileWriter(resultFileName);
					FileWriter fw2 = new FileWriter(top10FileName)) {
				for (InexQueryResult iqr : results) {
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
	
	private static List<PathCountTitle> loadFilePathCountTitle(
			String pathCountTitleFile) {
		LOGGER.log(Level.INFO, "Loading path-count-titles..");
		List<PathCountTitle> pathCountList = new ArrayList<PathCountTitle>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				pathCountTitleFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				try {
					if (!line.contains(","))
						continue;
					String[] fields = line.split(",");
					String path = ClusterDirectoryInfo.CLUSTER_BASE + fields[0];
					Integer count = Integer.parseInt(fields[1].trim());
					String title = fields[2].trim();
					pathCountList.add(new PathCountTitle(path, count, title));
				} catch (Exception e) {
					LOGGER.log(Level.WARNING, "Couldn't read PathCountTitle: "
							+ line + " cause: " + e.toString());
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return pathCountList;
	}

}
