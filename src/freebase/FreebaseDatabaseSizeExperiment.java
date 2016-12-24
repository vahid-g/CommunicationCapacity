package freebase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;

public class FreebaseDatabaseSizeExperiment {

	static class ExperimentResult {
		int[][] lostCount;
		int[][] foundCount;
	}

	static final int PARTITION_COUNT = 10;
	static final Logger LOGGER = Logger.getLogger(FreebaseDatabaseSizeExperiment.class.getName());
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

	public static void main(String[] args) {
		for (int i = 0; i < 5; i++) {
			double threshold = Double.parseDouble(args[1]);
			databaseSizeStratified(i, threshold);
		}
	}

	/**
	 * runs all relevant queries on a single table instance
	 * 
	 * @param tableName
	 * @param attribs
	 */
	public static void singleTable(String tableName) {
		String attribs[] = { "name", "description" };
		List<FreebaseQuery> queries = FreebaseDataManager.loadMsnQueriesByRelevantTable(tableName);
		String indexPath = INDEX_BASE + tableName + "/";
		String dataQuery = FreebaseDataManager.buildDataQuery(tableName, attribs);
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(dataQuery, attribs);
		FreebaseDataManager.createIndex(docs, attribs, indexPath);
		List<FreebaseQueryResult> fqrList = FreebaseDataManager.runFreebaseQueries(queries, indexPath);
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + tableName + "_top3.csv");
			for (FreebaseQueryResult fqr : fqrList) {
				FreebaseQuery query = fqr.freebaseQuery;
				fw.write(query.id + ", " + query.text + ", " + query.wiki + "," + query.fbid + "," + query.frequency
						+ ", " + fqr.precisionAtK(3) + ", " + fqr.top3Hits[0] + "," + fqr.top3Hits[1] + ", "
						+ fqr.top3Hits[2] + "\n");
			}
		} catch (Exception e) {
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

	/**
	 * database size experiment on a single table (this method is missing create
	 * index step) outputs: a file with rows associated with queries
	 * 
	 * @param tableName
	 */
	public static void databaseSize(String tableName) {
		LOGGER.log(Level.INFO, "Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager.loadMsnQueriesByRelevantTable(tableName);
		String indexPaths[] = new String[PARTITION_COUNT];
		for (int i = 0; i < PARTITION_COUNT; i++) {
			LOGGER.log(Level.INFO, "Building index " + i + "..");
			indexPaths[i] = INDEX_BASE + tableName + "_" + i + "/";
			// missing create index
		}
		LOGGER.log(Level.INFO, "submitting queries..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + tableName + "_mrr.csv");
			for (FreebaseQuery query : queries) {
				fw.write(query.id + ", " + query.text + ", " + query.frequency + ", ");
				for (int i = 0; i < PARTITION_COUNT; i++) {
					FreebaseQueryResult fqr = FreebaseDataManager.runFreebaseQuery(query, indexPaths[i]);
					fw.write(fqr.mrr() + ", ");
				}
				fw.write("\n");
			}
		} catch (Exception e) {
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
	public static void databaseSizeStratified(int expNo, double easeThreshold) {
		String tableName = "tbl_all";
		LOGGER.log(Level.INFO, "Loading queries..");
		String[] attribs = { "name", "description" };
		String sql = "select * from query_hardness_full where hardness < " + easeThreshold + ";";
		List<FreebaseQuery> queries = FreebaseDataManager.loadMsnQueriesFromSql(sql);

		String indexPaths[] = new String[PARTITION_COUNT];
		for (int i = 0; i < PARTITION_COUNT; i++)
			indexPaths[i] = INDEX_BASE + expNo + "_" + tableName + "_" + i + "/";

		LOGGER.log(Level.INFO, "Loading tuples..");
		String dataQuery = FreebaseDataManager.buildDataQuery(tableName, attribs);
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(dataQuery, attribs, 1000);
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
			FreebaseDataManager.createIndex(nonRels, (int) (nonRels.size() * (i + 1.0) / PARTITION_COUNT), attribs,
					indexPaths[i]);
			FreebaseDataManager.appendIndex(rels, (int) (rels.size() * (i + 1.0) / PARTITION_COUNT), attribs,
					indexPaths[i]);
		}
		LOGGER.log(Level.INFO, "Submitting Queries..");
		List<List<FreebaseQueryResult>> results = new ArrayList<List<FreebaseQueryResult>>();
		for (int i = 0; i < PARTITION_COUNT; i++) {
			List<FreebaseQueryResult> fqrList = FreebaseDataManager.runFreebaseQueries(queries, indexPaths[i]);
			results.add(fqrList);
		}

		LOGGER.log(Level.INFO, "Writing results to file..");
		FileWriter fw_p3 = null;
		FileWriter fw_mrr = null;
		try {
			fw_p3 = new FileWriter(
					RESULT_DIR + expNo + "_" + tableName + "_p3_stratified_hards_" + easeThreshold + ".csv");
			fw_mrr = new FileWriter(
					RESULT_DIR + expNo + "_" + tableName + "_mrr_stratified_hards_" + easeThreshold + ".csv");
			for (int i = 0; i < queries.size(); i++) {
				FreebaseQuery query = queries.get(i);
				fw_p3.write(query.id + ", " + query.text + ", " + query.frequency + ", ");
				fw_mrr.write(query.id + ", " + query.text + ", " + query.frequency + ", ");
				for (int j = 0; j < PARTITION_COUNT; j++) {
					FreebaseQueryResult fqr = results.get(j).get(i);
					if (fqr.freebaseQuery != query)
						LOGGER.log(Level.SEVERE, "----Alarm!");
					fw_p3.write(fqr.p3() + ", ");
					fw_mrr.write(fqr.mrr() + ", ");
				}
				fw_p3.write("\n");
				fw_mrr.write("\n");
			}
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
			if (fw_mrr != null) {
				try {
					fw_mrr.close();
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.toString());
				}
			}
		}
	}

	/**
	 * randomized database size experiment based on relevant/nonrelevant stratas
	 * on tbl_all table output: a file with rows associated with queries
	 * 
	 * @param tableName
	 * @param experimentNo
	 */
	public static void databaseSizeStratifiedTables(int experimentNo) {
		String tableName = "tbl_all";
		String attribs[] = { "name", "description" };
		LOGGER.log(Level.INFO, "Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager.loadMsnQueriesByRelevantTable(tableName);
		String indexPaths[] = new String[PARTITION_COUNT];
		LOGGER.log(Level.INFO, "Loading tuples into docs..");
		String indexQueryRel = FreebaseDataManager.buildDataQuery("tbl_all_rel", attribs);
		String indexQueryNrel = FreebaseDataManager.buildDataQuery("tbl_all_nrel", attribs);
		List<Document> relDocs = FreebaseDataManager.loadTuplesToDocuments(indexQueryRel + " order by frequency DESC",
				attribs);
		List<Document> nrelDocs = FreebaseDataManager.loadTuplesToDocuments(indexQueryNrel, attribs);
		Collections.shuffle(nrelDocs);
		for (int i = 0; i < PARTITION_COUNT; i++) {
			LOGGER.log(Level.INFO, "Building index " + i + "..");
			indexPaths[i] = INDEX_BASE + tableName + "_" + i + "/";
			int l = (int) (((i + 1.0) / PARTITION_COUNT) * nrelDocs.size());
			FreebaseDataManager.createIndex(nrelDocs, l, attribs, indexPaths[i]);
			int m = (int) (((i + 1.0) / PARTITION_COUNT) * relDocs.size());
			FreebaseDataManager.appendIndex(relDocs, m, attribs, indexPaths[i]);
		}
		LOGGER.log(Level.INFO, "Submitting queries..");
		List<List<FreebaseQueryResult>> resultList = new ArrayList<List<FreebaseQueryResult>>();
		for (int i = 0; i < PARTITION_COUNT; i++) {
			List<FreebaseQueryResult> fqrList = FreebaseDataManager.runFreebaseQueries(queries, indexPaths[i]);
			resultList.add(fqrList);
		}
		LOGGER.log(Level.INFO, "Writing queries to file..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + tableName + "_p3_rand_" + experimentNo + ".csv");
			for (int h = 0; h < queries.size(); h++) {
				FreebaseQuery query = queries.get(h);
				fw.write(query.id + ", " + query.text.replace("\"", "") + ", " + query.frequency + ", ");
				for (int i = 0; i < PARTITION_COUNT; i++) {
					FreebaseQueryResult fqr = resultList.get(i).get(h);
					fw.write(fqr.p3() + ", ");
				}
				fw.write("\n");
			}
		} catch (Exception e) {
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

	/**
	 * randomized database size experiment output: a file with rows associated
	 * with queries
	 * 
	 * @param tableName
	 * @param experimentNo
	 */
	public static void randomizedDatabaseSize(String tableName, int experimentNo) {
		String attribs[] = { "name", "description" };
		LOGGER.log(Level.INFO, "  Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager.loadMsnQueriesByRelevantTable(tableName);
		String indexPaths[] = new String[PARTITION_COUNT];
		LOGGER.log(Level.INFO, "  Loading tuples into docs..");

		String indexQuery = FreebaseDataManager.buildDataQuery(tableName, attribs);
		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(indexQuery, attribs);
		Collections.shuffle(docs);

		for (int i = 0; i < PARTITION_COUNT; i++) {
			indexPaths[i] = INDEX_BASE + tableName + "_" + i + "/";
			int l = (int) (((i + 1.0) / PARTITION_COUNT) * docs.size());
			FreebaseDataManager.createIndex(docs, l, attribs, indexPaths[i]);
		}
		LOGGER.log(Level.INFO, "  Submitting queries..");
		List<List<FreebaseQueryResult>> resultList = new ArrayList<List<FreebaseQueryResult>>();
		for (int i = 0; i < PARTITION_COUNT; i++) {
			List<FreebaseQueryResult> fqrList = FreebaseDataManager.runFreebaseQueries(queries, indexPaths[i]);
			resultList.add(fqrList);
		}
		LOGGER.log(Level.INFO, "  Writing queries to file..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + tableName + "_p3_rand_" + experimentNo + ".csv");
			for (int h = 0; h < queries.size(); h++) {
				FreebaseQuery query = queries.get(h);
				fw.write(query.id + ", " + query.text.replace("\"", "") + ", " + query.frequency + ", ");
				for (int i = 0; i < PARTITION_COUNT; i++) {
					FreebaseQueryResult fqr = resultList.get(i).get(h);
					fw.write(fqr.p3() + ", ");
				}
				fw.write("\n");
			}
		} catch (Exception e) {
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

	/**
	 * randomized database size query size experiment output
	 * 
	 * @param experimentNo
	 * @param queryPartitionCount
	 * @param dbPartitionCounts
	 * @param indexBase
	 * @param tableName
	 * 
	 * @return ExperimentResult object showing number of queries that has gained
	 *         and lost precision
	 */
	public static ExperimentResult randomizedDatabaseSizeQuerySize_ExperimentResult(int experimentNo,
			int queryPartitionCount, int dbPartitionCounts, String indexBase, String tableName) {
		String attribs[] = { "name", "description" };
		LOGGER.log(Level.INFO, experimentNo + ". Loading queries..");
		List<FreebaseQuery> queriesList = FreebaseDataManager.loadMsnQueriesByRelevantTable(tableName);
		long seed = System.nanoTime();
		Collections.shuffle(queriesList, new Random(seed));

		LOGGER.log(Level.INFO, experimentNo + ". Loading tuples into docs..");
		String indexPaths[] = new String[dbPartitionCounts];
		String indexQuery = FreebaseDataManager.buildDataQuery(tableName, attribs);

		List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(indexQuery, attribs);
		Collections.shuffle(docs);
		LOGGER.log(Level.INFO, experimentNo + ".Building index..");
		for (int i = 0; i < dbPartitionCounts; i++) {
			indexPaths[i] = INDEX_BASE + experimentNo + "_" + tableName + "_" + i + "/";
			int l = (int) (((i + 1.0) / dbPartitionCounts) * docs.size());
			FreebaseDataManager.createIndex(docs, l, attribs, indexPaths[i]);
		}

		LOGGER.log(Level.INFO, experimentNo + ". submitting queries..");
		int[][] foundCounter = new int[queryPartitionCount][dbPartitionCounts];
		int[][] lostCounter = new int[queryPartitionCount][dbPartitionCounts];
		for (int i = 0; i < queryPartitionCount; i++) {
			int endIndex = (int) ((i + 1.0) / queryPartitionCount * queriesList.size());
			List<FreebaseQuery> queries = queriesList.subList(0, endIndex);
			List<FreebaseQueryResult> oldResults = null;
			for (int j = 0; j < dbPartitionCounts; j++) {
				List<FreebaseQueryResult> queryResults = FreebaseDataManager.runFreebaseQueries(queries, indexPaths[j]);
				if (j == 0) {
					for (int k = 0; k < queryResults.size(); k++) {
						if (queryResults.get(k).p3() > 0)
							foundCounter[i][j]++;
					}
				} else {
					for (int k = 0; k < queryResults.size(); k++) {
						if (queryResults.get(k).p3() > oldResults.get(k).p3()) {
							foundCounter[i][j]++;
						} else if (queryResults.get(k).p3() < oldResults.get(k).p3()) {
							lostCounter[i][j]++;
						} else {
							// continue
						}
					}
				}
				oldResults = queryResults;
			}
		}
		ExperimentResult er = new ExperimentResult();
		er.lostCount = lostCounter;
		er.foundCount = foundCounter;
		return er;
	}

	/**
	 * repeated randomized database size query size experiment output: two files
	 * with rows as query set instances and columns as database instances
	 * showing average number of queries that gained and lost p@3
	 * 
	 */
	public static void repeatRandomizedDatabaseSizeQuerySize() {
		ExperimentResult[] er = new ExperimentResult[50];
		ExperimentResult fr = new ExperimentResult();
		int qCount = 5;
		int dCount = 10;
		int expCount = 10;
		fr.lostCount = new int[qCount][dCount];
		fr.foundCount = new int[qCount][dCount];
		for (int i = 0; i < expCount; i++) {
			LOGGER.log(Level.INFO, "====== exp iteration " + i);
			er[i] = FreebaseDatabaseSizeExperiment.randomizedDatabaseSizeQuerySize_ExperimentResult(i, qCount, dCount,
					INDEX_BASE, "media");
			fr.lostCount = Utils.addMatrix(fr.lostCount, er[i].lostCount);
			fr.foundCount = Utils.addMatrix(fr.foundCount, er[i].foundCount);
		}
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + "result_lost.csv");
			for (int i = 0; i < qCount; i++) {
				for (int j = 0; j < dCount - 1; j++) {
					fw.write(fr.lostCount[i][j] / ((double) expCount) + ",");
				}
				fw.write(fr.lostCount[i][dCount - 1] / ((double) expCount) + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		}
		try {
			fw.close();
		} catch (IOException e1) {
			LOGGER.log(Level.SEVERE, e1.toString());
		}
		try {
			fw = new FileWriter(RESULT_DIR + "result_found.csv");
			for (int i = 0; i < qCount; i++) {
				for (int j = 0; j < dCount - 1; j++) {
					fw.write(fr.foundCount[i][j] / ((double) expCount) + ",");
				}
				fw.write(fr.foundCount[i][dCount - 1] / ((double) expCount) + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		}
		try {
			fw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}