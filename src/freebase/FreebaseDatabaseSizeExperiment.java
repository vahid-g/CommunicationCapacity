package freebase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;

public class FreebaseDatabaseSizeExperiment {

	static final int PARTITION_COUNT = 10;
	static final Logger LOGGER = Logger
			.getLogger(FreebaseDatabaseSizeExperiment.class.getName());
	static final String INDEX_BASE = FreebaseDirectoryInfo.INDEX_DIR;
	static final String RESULT_DIR = FreebaseDirectoryInfo.RESULT_DIR;

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

	static class ExperimentConfig {
		String tableName = "tbl_all";
		String name = "exp";
		double hardness = 1; // this is exclusive
		double partitionPercentage = 1;
		String[] attribs = { "name", "description" };

		String getFullName() {
			return name + "_" + tableName + "_p" + partitionPercentage + "_h"
					+ hardness;
		}

		String getIndexDir() {
			return INDEX_BASE + tableName + "_p"
					+ (int) (partitionPercentage * 100) + "/";
		}
	}

	public static void main(String[] args) {
		// for (int i = 0; i < 5; i++) {
		// double threshold = Double.parseDouble(args[1]);
		// databaseSizeStratified(i, threshold);
		// }

		// singleTable("tbl_all");

//		ExperimentConfig config = new ExperimentConfig();
//		config.partitionPercentage = 0.3;
//		List<FreebaseQueryResult> result = partialSingleTable(config);
//		writeFreebaseQueryResults(result, config.getFullName()
//				+ "_weightonly.csv");
	}

	/**
	 * runs all relevant queries on a single table instance
	 * 
	 * @param tableName
	 * @param attribs
	 */
	public static List<FreebaseQueryResult> singleTable(ExperimentConfig config) {
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueriesByRelevantTable(config.tableName);
		String indexPath = INDEX_BASE + config.getFullName() + "/";
		String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
				config.attribs);
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
				dataQuery, config.attribs, Integer.MIN_VALUE);
		FreebaseDataManager.createIndex(docs, config.attribs, indexPath);
		List<FreebaseQueryResult> fqrList = FreebaseDataManager
				.runFreebaseQueries(queries, indexPath);
		return fqrList;
	}

	public static List<FreebaseQueryResult> partialSingleTable(
			ExperimentConfig config) {
		LOGGER.log(Level.INFO, "Loading queries..");
		String sql = "select * from query_hardness_full where hardness < "
				+ config.hardness + ";";
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueriesFromSql(sql);

		LOGGER.log(Level.INFO, "Loading tuples..");
		String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
				config.attribs);
		TreeMap<String, Integer> weights = FreebaseDataManager
				.loadQueryWeights();
		// System.out.println(Collections.max(weights.values()));
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
				dataQuery, config.attribs, FreebaseDataManager.MAX_FETCH,
				weights);
		Collections.sort(docs, new Comparator<Document>() {
			@Override
			public int compare(Document o1, Document o2) {
				Float w1 = Float.parseFloat(o1
						.get(FreebaseDataManager.FREQ_ATTRIB));
				Float w2 = Float.parseFloat(o2
						.get(FreebaseDataManager.FREQ_ATTRIB));
				return w2.compareTo(w1);
			}
		});
		LOGGER.log(
				Level.INFO,
				"Highest weight: "
						+ docs.get(0).get(FreebaseDataManager.FREQ_ATTRIB));
		LOGGER.log(Level.INFO, "Building index " + "..");
		String indexPaths = INDEX_BASE + config.getFullName() + "/";
		FreebaseDataManager.createIndex(docs,
				(int) (config.partitionPercentage * docs.size()),
				config.attribs, indexPaths);
		LOGGER.log(Level.INFO, "Submitting Queries..");
		List<FreebaseQueryResult> fqrList = FreebaseDataManager
				.runFreebaseQueries(queries, indexPaths);
		return fqrList;
	}

	/**
	 * 
	 * Selects a subset of queries based on hardness config param. Then selects
	 * a partition of database based on the partitionPercentage config param and
	 * builds index on it. This partition is based on equal percentages of
	 * relevant and non-relevant tuples. Then runs the selected queries on this
	 * indexed partition. Note that this method also considers weight for tuples
	 * (deduced based on weights of queries).
	 * 
	 * @param config
	 * @return
	 */
	public static List<FreebaseQueryResult> partialSingleTableMixed(
			ExperimentConfig config) {
		LOGGER.log(Level.INFO, "Loading queries..");
		String sql = "select * from query_hardness_full where hardness < "
				+ config.hardness + ";";
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueriesFromSql(sql);

		LOGGER.log(Level.INFO, "Loading tuples..");
		String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
				config.attribs);
		TreeMap<String, Integer> weights = FreebaseDataManager
				.loadQueryWeights();
		// System.out.println(Collections.max(weights.values()));
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
				dataQuery, config.attribs, FreebaseDataManager.MAX_FETCH,
				weights);
		Collections.shuffle(docs);
		LOGGER.log(Level.INFO, "All docs: {0}", docs.size());
		List<Document> rels = new ArrayList<Document>();
		List<Document> nonRels = new ArrayList<Document>();
		for (Document doc : docs) {
			String fbid = doc.get(FreebaseDataManager.FBID_ATTRIB);
			if (weights.containsKey(fbid))
				rels.add(doc);
			else
				nonRels.add(doc);
		}
		Collections.sort(rels, new Comparator<Document>() {
			@Override
			public int compare(Document o1, Document o2) {
				Float w1 = Float.parseFloat(o1
						.get(FreebaseDataManager.FREQ_ATTRIB));
				Float w2 = Float.parseFloat(o2
						.get(FreebaseDataManager.FREQ_ATTRIB));
				return w2.compareTo(w1);
			}
		});
		LOGGER.log(
				Level.INFO,
				"Highest weight: "
						+ rels.get(0).get(FreebaseDataManager.FREQ_ATTRIB));
		docs = null;
		LOGGER.log(Level.INFO, "NonRel docs: {0}", nonRels.size());
		LOGGER.log(Level.INFO, "Rel docs: {0}", rels.size());
		LOGGER.log(Level.INFO, "All docs: {0}", rels.size() + nonRels.size());
		LOGGER.log(Level.INFO, "Building index " + "..");
		String indexPaths = INDEX_BASE + config.getFullName() + "/";
		FreebaseDataManager.createIndex(nonRels,
				(int) (config.partitionPercentage * nonRels.size()),
				config.attribs, indexPaths);
		FreebaseDataManager.appendIndex(rels,
				(int) (config.partitionPercentage * rels.size()),
				config.attribs, indexPaths);
		LOGGER.log(Level.INFO, "Submitting Queries..");
		List<FreebaseQueryResult> fqrList = FreebaseDataManager
				.runFreebaseQueries(queries, indexPaths);
		return fqrList;
	}

	/**
	 * database size experiment on a single table (this method is missing create
	 * index step)
	 * 
	 * @param tableName
	 * @return a list of FreebaseQueryResults objects.
	 */
	public static List<List<FreebaseQueryResult>> databaseSize(
			ExperimentConfig config) {
		LOGGER.log(Level.INFO, "Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueriesByRelevantTable(config.tableName);
		String indexPaths[] = new String[PARTITION_COUNT];
		for (int i = 0; i < PARTITION_COUNT; i++) {
			LOGGER.log(Level.INFO, "Building index " + i + "..");
			indexPaths[i] = INDEX_BASE + config.tableName + "_" + i + "/";
			// missing create index
		}
		LOGGER.log(Level.INFO, "Submitting queries..");
		List<List<FreebaseQueryResult>> resultList = new ArrayList<List<FreebaseQueryResult>>();
		for (int i = 0; i < PARTITION_COUNT; i++) {
			List<FreebaseQueryResult> fqrList = FreebaseDataManager
					.runFreebaseQueries(queries, indexPaths[i]);
			resultList.add(fqrList);
		}
		return resultList;
	}

	/**
	 * Database size experiment on a single table with selected query subset.
	 * Picking different query subsets will result in different partitionings!
	 * 
	 * outputs: a file with rows associated with queries
	 * 
	 * @param tableName
	 * @param easeThreshold
	 *            a number between 0 and 1 specifying easiness of selected
	 *            queries.
	 * 
	 */
	public static List<List<FreebaseQueryResult>> databaseSizeStratified(
			ExperimentConfig config) {
		LOGGER.log(Level.INFO, "Loading queries..");
		String sql = "select * from query_hardness_full where hardness < "
				+ config.hardness + ";";
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueriesFromSql(sql);

		String indexPaths[] = new String[PARTITION_COUNT];
		for (int i = 0; i < PARTITION_COUNT; i++)
			indexPaths[i] = INDEX_BASE + config.name + "_" + config.tableName
					+ "_" + i + "/";

		LOGGER.log(Level.INFO, "Loading tuples..");
		String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
				config.attribs);
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
				dataQuery, config.attribs, 1000);
		Collections.shuffle(docs);
		LOGGER.log(Level.INFO, "All docs: {0}", docs.size());
		List<Document> rels = new ArrayList<Document>();
		List<Document> nonRels = new ArrayList<Document>();
		TreeSet<String> relFbids = new TreeSet<String>();
		for (FreebaseQuery query : queries)
			relFbids.add(query.fbid);
		for (Document doc : docs) {
			String fbid = doc.get(FreebaseDataManager.FBID_ATTRIB);
			if (relFbids.contains(fbid))
				rels.add(doc);
			else
				nonRels.add(doc);
		}
		docs = null;
		LOGGER.log(Level.INFO, "NonRel docs: {0}", nonRels.size());
		LOGGER.log(Level.INFO, "Rel docs: {0}", rels.size());
		LOGGER.log(Level.INFO, "All docs: {0}", rels.size() + nonRels.size());
		for (int i = 0; i < PARTITION_COUNT; i++) {
			LOGGER.log(Level.INFO, "Building index " + i + "..");
			FreebaseDataManager.createIndex(nonRels, (int) (nonRels.size()
					* (i + 1.0) / PARTITION_COUNT), config.attribs,
					indexPaths[i]);
			FreebaseDataManager.appendIndex(rels, (int) (rels.size()
					* (i + 1.0) / PARTITION_COUNT), config.attribs,
					indexPaths[i]);
		}
		LOGGER.log(Level.INFO, "Submitting Queries..");
		List<List<FreebaseQueryResult>> results = new ArrayList<List<FreebaseQueryResult>>();
		for (int i = 0; i < PARTITION_COUNT; i++) {
			List<FreebaseQueryResult> fqrList = FreebaseDataManager
					.runFreebaseQueries(queries, indexPaths[i]);
			results.add(fqrList);
		}
		return results;
	}

	/**
	 * randomized database size experiment based on relevant/nonrelevant stratas
	 * on tbl_all table.
	 * 
	 * @param tableName
	 * @param experimentNo
	 * 
	 * @return a list of FreebaseQueryResult objects
	 */

	public static List<List<FreebaseQueryResult>> databaseSizeStratifiedTables(
			ExperimentConfig config) {
		LOGGER.log(Level.INFO, "Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager
				.loadMsnQueriesByRelevantTable(config.tableName);
		String indexPaths[] = new String[PARTITION_COUNT];
		LOGGER.log(Level.INFO, "Loading tuples into docs..");
		String indexQueryRel = FreebaseDataManager.buildDataQuery(
				"tbl_all_rel", config.attribs);
		String indexQueryNrel = FreebaseDataManager.buildDataQuery(
				"tbl_all_nrel", config.attribs);
		List<Document> relDocs = FreebaseDataManager.loadTuplesToDocuments(
				indexQueryRel + " order by frequency DESC", config.attribs);
		List<Document> nrelDocs = FreebaseDataManager.loadTuplesToDocuments(
				indexQueryNrel, config.attribs);
		Collections.shuffle(nrelDocs);
		for (int i = 0; i < PARTITION_COUNT; i++) {
			LOGGER.log(Level.INFO, "Building index " + i + "..");
			indexPaths[i] = INDEX_BASE + config.tableName + "_" + i + "/";
			int l = (int) (((i + 1.0) / PARTITION_COUNT) * nrelDocs.size());
			FreebaseDataManager.createIndex(nrelDocs, l, config.attribs,
					indexPaths[i]);
			int m = (int) (((i + 1.0) / PARTITION_COUNT) * relDocs.size());
			FreebaseDataManager.appendIndex(relDocs, m, config.attribs,
					indexPaths[i]);
		}
		LOGGER.log(Level.INFO, "Submitting queries..");
		List<List<FreebaseQueryResult>> resultList = new ArrayList<List<FreebaseQueryResult>>();
		for (int i = 0; i < PARTITION_COUNT; i++) {
			List<FreebaseQueryResult> fqrList = FreebaseDataManager
					.runFreebaseQueries(queries, indexPaths[i]);
			resultList.add(fqrList);
		}
		return resultList;
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
	 *            the input list
	 * @param resultFileName
	 *            : output csv file name
	 */
	static void writeFreebaseQueryResultMatrix(
			List<List<FreebaseQueryResult>> results, String resultFile) {
		LOGGER.log(Level.INFO, "Writing results to file..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(resultFile);
			for (int i = 0; i < results.get(0).size(); i++) {
				FreebaseQuery query = results.get(i).get(0).freebaseQuery;
				fw.write(query.id + ", " + query.text + ", " + query.frequency
						+ ", ");
				for (int j = 0; j < results.size(); j++) {
					FreebaseQueryResult fqr = results.get(j).get(i);
					if (fqr.freebaseQuery != query)
						LOGGER.log(Level.SEVERE, "----Alarm!");
					fw.write(fqr.p3() + ", ");
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