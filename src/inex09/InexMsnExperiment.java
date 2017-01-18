package inex09;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import freebase.FreebaseDatabaseSizeExperiment;

public class InexMsnExperiment {

	static final String DATASET_PATH = "/scratch/data-sets/inex_09/";
	static final String FILE_COUNT_FILE_PATH = "data/file_count.csv";
	static final String QUERY_QID_FILE_PATH = "data/queries/msn/query_qid.csv";
	static final String QID_QREL_FILE_PATH = "data/queries/msn/qid_qrel.csv";
	static final String INDEX_BASE = "data/index/index09/";
	static final String RESULT_DIR = "data/result/";

	// static final String DATA_DIR = "/scratch/cluster-share/ghadakcv/";
	// static final String DATASET_PATH = DATA_DIR + "inex_09";
	// static final String FILE_COUNT_FILE_PATH = DATA_DIR + "file_count.csv";
	// static final String QUERY_QID_FILE_PATH = DATA_DIR
	// + "queries/msn/query_qid.csv";
	// static final String QID_QREL_FILE_PATH = DATA_DIR
	// + "queries/msn/qid_qrel.csv";
	// static final String INDEX_BASE = DATA_DIR + "index/index09/";
	// static final String RESULT_DIR = DATA_DIR + "result/";

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
		// int expNo = Integer.parseInt(args[0]);
		for (int i = 1; i <= 10; i++) {
			int expNo = i;
			long start_t = System.currentTimeMillis();
			exp(expNo);
			long end_t = System.currentTimeMillis();
			LOGGER.log(Level.INFO, "Time spent for experiment " + expNo
					+ " is " + (end_t - start_t) / 1000);
		}
	}

	public static void exp(int expNo) {
		LOGGER.log(Level.INFO, "Loading files list and counts");
		Map<String, Integer> fileCountMap = new HashMap<String, Integer>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				FILE_COUNT_FILE_PATH))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (!line.contains(","))
					continue;
				String[] splits = line.split(",");
				fileCountMap.put(splits[0], Integer.parseInt(splits[1].trim()));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		LOGGER.log(Level.INFO, "Sorting files..");
		int size = (int) (fileCountMap.size() * (expNo / 10.0));
		Map<String, Integer> fileCountSorted = Utils.sortByValue(fileCountMap,
				size);

		String indexName = INDEX_BASE + "index_inex_" + expNo;
		LOGGER.log(Level.INFO, "Building index..");
		InexIndexer.buildIndex(fileCountSorted, indexName);

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
