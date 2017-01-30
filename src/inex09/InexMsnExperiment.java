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
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import freebase.FreebaseDatabaseSizeExperiment;

public class InexMsnExperiment {

//	static final String DATASET_PATH = "/scratch/data-sets/inex_09/";
//	static final String PATH_COUNT_FILE = "data/file_count.csv";
//	static final String QUERY_QID_FILE_PATH = "data/queries/msn/query_qid.csv";
//	static final String QID_QREL_FILE_PATH = "data/queries/msn/qid_qrel.csv";
//	static final String INDEX_BASE = "data/index/index09/";
//	static final String RESULT_DIR = "data/result/";

	static final String CLUSTER_BASE = "/scratch/cluster-share/ghadakcv/";
	static final String DATASET_PATH = CLUSTER_BASE + "inex_09";
	static final String PATH_COUNT_FILE = CLUSTER_BASE + "data/file_count.csv";
	static final String QUERY_QID_FILE_PATH = CLUSTER_BASE + "data/msn/query_qid.csv";
	static final String QID_QREL_FILE_PATH = CLUSTER_BASE + "data/msn/qid_qrel.csv";
	static final String INDEX_BASE = "/scratch/ghadakcv/index09/";
	static final String RESULT_DIR = CLUSTER_BASE + "data/result/";

	static final Logger LOGGER = Logger
			.getLogger(FreebaseDatabaseSizeExperiment.class.getName());
	static {
		LOGGER.setUseParentHandlers(false);
		Handler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		LOGGER.addHandler(handler);
		LOGGER.setLevel(Level.ALL);

		File indexDir = new File(INDEX_BASE);
		if (!indexDir.exists())
			indexDir.mkdirs();
		File resultDir = new File(RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();
	}

	public static void main(String[] args) {
		int expNo = Integer.parseInt(args[0]);
		long start_t = System.currentTimeMillis();
		exp(expNo);
		long end_t = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "Time spent for experiment " + expNo + " is "
				+ (end_t - start_t) / 1000);
	}

	public static void exp(int expNo) {
		LOGGER.log(Level.INFO, "Loading files list and counts");
		Map<String, Integer> pathCountMap = new HashMap<String, Integer>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				PATH_COUNT_FILE))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (!line.contains(","))
					continue;
				String path = CLUSTER_BASE + line.split(",")[0];
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
		Map<String, Integer> pathCountSorted = Utils.sortByValue(pathCountMap,
				subsetSize);

		String indexName = INDEX_BASE + "index_inex_" + expNo;
		LOGGER.log(Level.INFO, "Building index..");
		InexIndexer.buildIndex(pathCountSorted, indexName);

		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(
				QUERY_QID_FILE_PATH, QID_QREL_FILE_PATH);
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries,
				indexName);

		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(RESULT_DIR + "inex_" + expNo
				+ ".csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write(mqr.msnQuery.text + ", " + mqr.precisionAtK(3) + ", "
						+ mqr.mrr() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void exp0() {
		List<String> allFiles = Utils
				.listFilesForFolder(new File(DATASET_PATH));
		InexIndexer.buildIndex(allFiles.toArray(new String[0]), INDEX_BASE);
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(
				QUERY_QID_FILE_PATH, QID_QREL_FILE_PATH);
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries,
				INDEX_BASE);
		try (FileWriter fw = new FileWriter(RESULT_DIR + "inex.csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write(mqr.msnQuery.text + ", " + mqr.precisionAtK(3) + ", "
						+ mqr.mrr() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
