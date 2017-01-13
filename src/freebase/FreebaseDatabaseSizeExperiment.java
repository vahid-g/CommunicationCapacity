package freebase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;

/**
 * @author vahid
 *
 */
public class FreebaseDatabaseSizeExperiment {

	static final int PARTITION_COUNT = 10;
	static final Logger LOGGER = Logger
			.getLogger(FreebaseDatabaseSizeExperiment.class.getName());
	static final String INDEX_BASE = FreebaseDirectoryInfo.INDEX_DIR;
	static final String RESULT_DIR = FreebaseDirectoryInfo.RESULT_DIR;

	static final int TBL_ALL_SIZE = 6046089;

	// initializing
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

	public static final class DocumentFreqComparator
			implements
				Comparator<Document> {
		@Override
		public int compare(Document o1, Document o2) {
			Float w1 = Float
					.parseFloat(o1.get(FreebaseDataManager.FREQ_ATTRIB));
			Float w2 = Float
					.parseFloat(o2.get(FreebaseDataManager.FREQ_ATTRIB));
			return w2.compareTo(w1);
		}
	}

	static class ExperimentConfig {
		String tableName = "tbl_all";
		String queryTableName = "query_all_mrr_weighted";
		String[] attribs = {"name", "description"};
		String name = "exp";
		double maxMrr = 1; // this is exclusive
		double trainSize = 0.5;
		double partitionSize = 0.5;
		float smoothingParam = 1;

		String getFullName() {
			return name + "_" + tableName + "_p" + partitionSize + "_h"
					+ maxMrr + "_sp" + smoothingParam;
		}

		String getIndexDir() {
			return INDEX_BASE + tableName + "_p" + (int) (partitionSize * 100)
					+ "/";
		}
	}

	public static void main(String[] args) {

		ExperimentConfig config = new ExperimentConfig();

		// // for computing mrr of queries using weighted tuples
		// List<FreebaseQueryResult> fqrList = weightedTableAllQueries(config);
		// writeFreebaseQueryResults(fqrList, "exp_tbl_all_weighted.csv");

		// // for running hard queries on a selected partition
		// List<FreebaseQueryResult> fqrList =
		// weightedTablePartitionHardQueries(config);
		// writeFreebaseQueryResults(fqrList, config.getFullName() + ".csv");

		// for computing optimal database size
		Map<FreebaseQuery, List<FreebaseQueryResult>> fqrMap = databaseSizeOptimal(
				config, 100);
		writeFreebaseQueryResults(fqrMap, config.getFullName());

		// // for generating sampling based results
		// Map<FreebaseQuery, List<FreebaseQueryResult>> fqrMap = databaseSize(
		// config, 100);
		// writeFreebaseQueryResults(fqrMap, "exp_dbsize_learned_p100_h1.csv");
	}

	/**
	 * runs all queries from "query" table on a single table instance
	 * 
	 * @param tableName
	 * @param attribs
	 */
	public static List<FreebaseQueryResult> singleTable(ExperimentConfig config) {
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueries("query_all");
		String indexPath = config.getIndexDir();
		String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
				config.attribs);
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
				dataQuery, config.attribs, FreebaseDataManager.MAX_FETCH);
		FreebaseDataManager.createIndex(docs, config.attribs, indexPath);
		List<FreebaseQueryResult> fqrList = FreebaseDataManager
				.runFreebaseQueries(queries, indexPath);
		return fqrList;
	}

	/**
	 * runs all queries from "query" table on a single table instance
	 * considering weights
	 * 
	 * @param tableName
	 * @param attribs
	 */
	public static List<FreebaseQueryResult> weightedTableAllQueries(
			ExperimentConfig config) {
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueries("query_all");
		return weightedTablePartition(config, queries, queries);
	}

	/**
	 * Load tuple weights from the query log and runs the queries on a partition
	 * of the table. Size of partition is decided based on config.
	 * 
	 * @param config
	 * @return List of FreebaseQueryResult
	 */
	public static List<FreebaseQueryResult> weightedTablePartitionHardQueries(
			ExperimentConfig config) {
		LOGGER.log(Level.INFO, "Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueriesWithMaxMrr(config.queryTableName, config.maxMrr);
		return weightedTablePartition(config, queries, queries);
	}

	/**
	 * Estimates tuples wieghts based on train query set, creates index and runs
	 * the test queries over this index
	 * 
	 * @param config
	 * @param trainQueries
	 * @param testQueries
	 * @return
	 */
	public static List<FreebaseQueryResult> weightedTablePartition(
			ExperimentConfig config, List<FreebaseQuery> trainQueries,
			List<FreebaseQuery> testQueries) {
		LOGGER.log(Level.INFO, "Loading tuples..");
		String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
				config.attribs);
		TreeMap<String, Float> weights = FreebaseDataManager
				.loadQueryWeights(trainQueries);
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
				dataQuery, config.attribs, FreebaseDataManager.MAX_FETCH,
				weights);
		// Collections.shuffle(docs);
		Collections.sort(docs, new DocumentFreqComparator());
		LOGGER.log(Level.INFO, "Building index " + "..");
		String indexPaths = config.getIndexDir();
		FreebaseDataManager.createIndex(docs,
				(int) (config.partitionSize * docs.size()), config.attribs,
				indexPaths);
		LOGGER.log(Level.INFO, "Submitting Queries..");
		List<FreebaseQueryResult> fqrList = FreebaseDataManager
				.runFreebaseQueries(testQueries, indexPaths);
		return fqrList;
	}

	/**
	 * database size experiment on a single table
	 * 
	 * @param tableName
	 * @return a list of FreebaseQueryResults objects.
	 */
	public static Map<FreebaseQuery, List<FreebaseQueryResult>> databaseSizeOptimal(
			ExperimentConfig config, int partitionCount) {
		LOGGER.log(Level.INFO, "Loading queries..");
		List<FreebaseQuery> queryList = FreebaseDataManager
				.loadMsnQueriesWithMaxMrr(config.queryTableName, config.maxMrr);
		TreeMap<String, Float> weights = FreebaseDataManager
				.loadQueryWeights(queryList);
		String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
				config.attribs);
		List<Document> docs = FreebaseDataManager
				.loadTuplesToDocumentsSmoothing(dataQuery, config.attribs,
						FreebaseDataManager.MAX_FETCH, weights, 1.0,
						TBL_ALL_SIZE);
		// Collections.shuffle(docs);
		Collections.sort(docs, new DocumentFreqComparator());
		LOGGER.log(Level.INFO, "max weight: ${0}",
				docs.get(0).get(FreebaseDataManager.FREQ_ATTRIB));
		String indexPaths[] = new String[partitionCount];
		Map<FreebaseQuery, List<FreebaseQueryResult>> results = new HashMap<FreebaseQuery, List<FreebaseQueryResult>>();
		for (FreebaseQuery query : queryList) {
			results.put(query, new ArrayList<FreebaseQueryResult>());
		}
		for (int i = 0; i < partitionCount; i++) {
			// if (i == 0 || i == partitionCount - 1) {
			LOGGER.log(Level.INFO, "Building index " + i + "..");
			indexPaths[i] = INDEX_BASE + config.tableName + "_" + i + "/";
			FreebaseDataManager.createIndex(docs,
					(int) (((i + 1.0) / partitionCount) * docs.size()),
					config.attribs, indexPaths[i]);
			LOGGER.log(Level.INFO, "Submitting queries..");
			List<FreebaseQueryResult> resultList = FreebaseDataManager
					.runFreebaseQueries(queryList, indexPaths[i]);
			Map<FreebaseQuery, FreebaseQueryResult> resultMap = FreebaseDataManager
					.convertResultListToMap(resultList);
			for (FreebaseQuery query : queryList) {
				List<FreebaseQueryResult> list = results.get(query);
				list.add(resultMap.get(query));
			}
			// }

		}
		return results;
	}

	/**
	 * database size experiment on a single table (this method is missing create
	 * index step)
	 * 
	 * @param tableName
	 * @return a list of FreebaseQueryResults objects.
	 */
	public static Map<FreebaseQuery, List<FreebaseQueryResult>> databaseSize(
			ExperimentConfig config, int partitionCount) {
		LOGGER.log(Level.INFO, "Loading queries..");
		List<FreebaseQuery> queryList = FreebaseDataManager
				.loadMsnQueriesWithMaxMrr(config.queryTableName, config.maxMrr);
		List<FreebaseQuery> flatQueryList = Utils
				.flattenFreebaseQueries(queryList);
		// random sampling
		Collections.shuffle(flatQueryList);
		List<FreebaseQuery> trainQueries = flatQueryList.subList(0,
				(int) (flatQueryList.size() * config.trainSize));
		List<FreebaseQuery> testQueries = new ArrayList<FreebaseQuery>();
		testQueries.addAll(flatQueryList);
		testQueries.removeAll(trainQueries);
		LOGGER.log(Level.INFO, "train size: " + trainQueries.size());
		LOGGER.log(Level.INFO, "test size: " + testQueries.size());
		LOGGER.log(Level.INFO, "Loading tuples..");
		String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
				config.attribs);
		TreeMap<String, Float> weights = FreebaseDataManager
				.loadQueryInstanceWeights(trainQueries);
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
				dataQuery, config.attribs, FreebaseDataManager.MAX_FETCH,
				weights, config.smoothingParam);
		Collections.shuffle(docs);
		Collections.sort(docs, new DocumentFreqComparator());

		String indexPaths[] = new String[partitionCount];
		Map<FreebaseQuery, List<FreebaseQueryResult>> results = new HashMap<FreebaseQuery, List<FreebaseQueryResult>>();
		for (FreebaseQuery query : testQueries) {
			results.put(query, new ArrayList<FreebaseQueryResult>());
		}
		for (int i = 0; i < partitionCount; i++) {
			if (i != 0 && i != partitionCount - 1)
				continue;
			LOGGER.log(Level.INFO, "Building index " + i + "..");
			indexPaths[i] = INDEX_BASE + config.tableName + "_" + i + "/";
			FreebaseDataManager.createIndex(docs,
					(int) (((i + 1.0) / partitionCount) * docs.size()),
					config.attribs, indexPaths[i]);
			LOGGER.log(Level.INFO, "Submitting queries..");
			List<FreebaseQueryResult> resultList = FreebaseDataManager
					.runFreebaseQueries(testQueries, indexPaths[i]);
			Map<FreebaseQuery, FreebaseQueryResult> resultMap = FreebaseDataManager
					.convertResultListToMap(resultList);
			for (FreebaseQuery query : testQueries) {
				List<FreebaseQueryResult> list = results.get(query);
				list.add(resultMap.get(query));
			}
		}
		return results;
	}

	/**
	 * Writes a list of FreebaseQeueryResults to a csv file.
	 * 
	 * @param fqrList
	 *            : the input list
	 * @param resultFileName
	 *            : output csv file name
	 */
	static void writeFreebaseQueryResults(List<FreebaseQueryResult> fqrList,
			String resultFileName) {
		LOGGER.log(Level.INFO, "Writing results to file..");
		FileWriter fw_p3 = null;
		try {
			fw_p3 = new FileWriter(RESULT_DIR + resultFileName);
			for (FreebaseQueryResult fqr : fqrList) {
				FreebaseQuery query = fqr.freebaseQuery;
				fw_p3.write(query.id + ", " + query.text + ", "
						+ query.frequency + ", ");
				fw_p3.write(fqr.p3() + ", " + fqr.mrr() + "\n");
			}
			fw_p3.write("\n");
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} finally {
			if (fw_p3 != null) {
				try {
					fw_p3.close();
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.toString());
				}
			}
		}
	}

	/**
	 * Writes a list of FreebaseQeueryResults to a csv file.
	 * 
	 * @param fqrList
	 *            : the input list
	 * @param resultFileName
	 *            : output csv file name
	 */
	static void writeFreebaseQueryResults(
			Map<FreebaseQuery, List<FreebaseQueryResult>> fqrMap,
			String resultFileName) {
		LOGGER.log(Level.INFO, "Writing results to file..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + resultFileName);
			for (FreebaseQuery query : fqrMap.keySet()) {
				fw.write(query.id + ", " + query.text + ", " + query.frequency
						+ ", " + query.instanceId);
				List<FreebaseQueryResult> list = fqrMap.get(query);
				for (FreebaseQueryResult fqr : list) {
					fw.write(", " + fqr.mrr());
				}
				fw.write("\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.toString());
				}
			}
		}
	}

}