package freebase_experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.lucene.document.Document;

public class FreebaseExperiment {

	static class ExperimentResult {
		int[][] lostCount;
		int[][] foundCount;
	}

	final static String DATA_FOLDER = "data/";
	final static String INDEX_BASE = DATA_FOLDER + "freebase_index/";

	final static int PARTITION_COUNT = 10;

	public static void main(String[] args) {
		experiment_repeatRandomizedDatabaseSizeQuerySize();
	}

	// runs all queries on a single table instance
	public static void experiment_singleTable(String tableName, String[] attribs) {
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesByRelevancyTable(tableName);
		String indexPath = FreebaseExperiment.INDEX_BASE + tableName + "/";
		String dataQuery = FreebaseDataManager.buildDataQuery(tableName,
				attribs);
		FreebaseDataManager.createIndex(
				FreebaseDataManager.loadTuplesToDocuments(dataQuery, attribs),
				attribs, indexPath);
		FileWriter fw = null;
		try {
			fw = new FileWriter(FreebaseDataManager.resultDir + tableName
					+ ".csv");
			for (FreebaseQuery query : queries) {
				FreebaseDataManager.runQuery(query, indexPath);
				fw.write(query.id + ", " + query.text + ", " + query.frequency
						+ ", " + query.wiki + ", " + query.p3() + ", "
						+ query.precisionAtK(3) + ", " + query.mrr() + ","
						+ query.hits[0] + ", " + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	// schema capacity experiment. queries selected based on specific set of
	// keywords and submitted to a smaller table with keywords dropped
	public static void experiment_schemaCapacitySmallerTable(String tableName,
			String pattern) {
		// String[] table = { "tbl_tv_program", "tbl_album", "tbl_book" };
		// String[] pattern = {
		// "program| tv| television| serie| show | show$| film | film$| movie",
		// "music|record|song|sound| art |album",
		// "book|theme|novel|notes|writing|manuscript|story" };

		String attribs[] = { FreebaseDataManager.NAME_ATTRIB,
				FreebaseDataManager.DESC_ATTRIB };
		String indexPath = FreebaseExperiment.INDEX_BASE + tableName + "/";
		// createIndex(tableName, attribs, indexPath);
		String sql = "select * from query where text REGEXP '" + pattern
				+ "' and fbid in (select fbid from " + tableName + ");";
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesBySqlQuery(sql);
		FreebaseDataManager.removeKeyword(queries, pattern);
		try (FileWriter fw = new FileWriter(FreebaseDataManager.resultDir
				+ "t-" + tableName + " q-" + tableName + " a-name" + ".csv");) {
			for (FreebaseQuery query : queries) {
				FreebaseDataManager.runQuery(query, indexPath);
				fw.write(query.id + ", " + query.text + ", " + query.wiki
						+ ", " + query.p3() + ", " + query.precisionAtK(10)
						+ ", " + query.precisionAtK(20) + ", " + query.mrr()
						+ "," + query.hits[0] + ", " + query.hits[1] + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// schema capacity experiment. queries selected based on specific set of
	// keywords and submitted to a larger table. Note that the keywords are not
	// dropped.
	public static void experiment_schemaCapacityLargerTable(String tableName,
			String pattern, String queryTableName) {
		String attribs[] = { FreebaseDataManager.NAME_ATTRIB,
				FreebaseDataManager.DESC_ATTRIB,
				FreebaseDataManager.SEMANTIC_TYPE_ATTRIB };
		String indexPath = FreebaseExperiment.INDEX_BASE + tableName + "/";
		// createIndex(tableName, attribs, indexPath);
		String sql = "select * from query where text REGEXP '" + pattern
				+ "' and fbid in (select fbid from " + queryTableName + ");";
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesBySqlQuery(sql);
		FreebaseDataManager.annotateSemanticType(queries, pattern);
		try (FileWriter fw = new FileWriter(FreebaseDataManager.resultDir
				+ "t-" + tableName + " q-" + queryTableName + " a-name"
				+ ".csv");) {
			for (FreebaseQuery query : queries) {
				FreebaseDataManager.runQuery(query, indexPath);
				fw.write(query.id + ", " + query.text + ", " + query.wiki
						+ ", " + query.p3() + ", " + query.precisionAtK(10)
						+ ", " + query.precisionAtK(20) + ", " + query.mrr()
						+ "," + query.hits[0] + ", " + query.hits[1] + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// database size experiment on a single table (this method is missing create
	// index step)
	// output: a file with rows associated with queries
	public static void experiment_databaseSize(String tableName) {
		String attribs[] = { "name", "description" };
		System.out.println("Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesByRelevancyTable(tableName);
		String indexPaths[] = new String[PARTITION_COUNT];
		for (int i = 0; i < PARTITION_COUNT; i++) {
			System.out.println("Building index " + i + "..");
			indexPaths[i] = FreebaseExperiment.INDEX_BASE + tableName + "_" + i
					+ "/";
			// missing create index
		}
		System.out.println("submitting queries..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(FreebaseDataManager.resultDir + tableName
					+ "_mrr.csv");
			for (FreebaseQuery query : queries) {
				fw.write(query.id + ", " + query.text + ", " + query.frequency
						+ ", ");
				for (int i = 0; i < PARTITION_COUNT; i++) {
					FreebaseQueryResult fqr = FreebaseDataManager
							.runFreebaseQuery(query, indexPaths[i]);
					fw.write(fqr.mrr() + ", ");
				}
				fw.write("\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// randomized database size experiment
	// output: a file with rows associated with queries
	public static void experiment_databaseSizeRandomized(String tableName,
			int experimentNo) {
		String attribs[] = { "name", "description" };
		System.out.println("Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesByRelevancyTable(tableName);
		String indexPaths[] = new String[PARTITION_COUNT];
		System.out.println("Loading tuples into docs..");
		String indexQuery = FreebaseDataManager.buildDataQuery(tableName,
				attribs);
		Document[] docs = FreebaseDataManager.loadTuplesToDocuments(indexQuery,
				attribs);
		Utils.shuffleArray(docs);

		for (int i = 0; i < PARTITION_COUNT; i++) {
			System.out.println("Building index " + i + "..");
			indexPaths[i] = FreebaseExperiment.INDEX_BASE + tableName + "_" + i
					+ "/";
			int l = (int) (((i + 1.0) / PARTITION_COUNT) * docs.length);
			FreebaseDataManager.createIndex(Arrays.copyOf(docs, l), attribs,
					indexPaths[i]);
		}
		System.out.println("submitting queries..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(FreebaseDataManager.resultDir + tableName
					+ "_randomized_p3_" + experimentNo + ".csv");
			for (FreebaseQuery query : queries) {
				fw.write(query.id + ", " + query.text.replace("\"", "") + ", "
						+ query.frequency + ", ");
				for (int i = 0; i < PARTITION_COUNT; i++) {
					FreebaseQueryResult fqr = FreebaseDataManager
							.runFreebaseQuery(query, indexPaths[i]);
					fw.write(fqr.p3() + ", ");
				}
				fw.write("\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// randomized database size query size experiment
	// output: An ExperimentResult object showing number of queries that has
	// gained and lost precision
	public static ExperimentResult experiment_randomizedDatabaseSizeQuerySize(
			int experimentNo, int queryPartitionCount, int dbPartitionCounts,
			String indexBase, String tableName) {
		String attribs[] = { "name", "description" };
		System.out.println(experimentNo + ". Loading queries..");
		List<FreebaseQuery> queriesList = FreebaseDataManager
				.getQueriesByRelevancyTable(tableName);
		long seed = System.nanoTime();
		Collections.shuffle(queriesList, new Random(seed));

		System.out.println(experimentNo + ". Loading tuples into docs..");
		String indexPaths[] = new String[dbPartitionCounts];
		String indexQuery = FreebaseDataManager.buildDataQuery(tableName,
				attribs);
		Document[] docs = FreebaseDataManager.loadTuplesToDocuments(indexQuery,
				attribs);
		Utils.shuffleArray(docs);

		System.out.println(experimentNo + ".Building index..");
		for (int i = 0; i < dbPartitionCounts; i++) {
			indexPaths[i] = INDEX_BASE + experimentNo + "_" + tableName + "_"
					+ i + "/";
			int l = (int) (((i + 1.0) / dbPartitionCounts) * docs.length);
			FreebaseDataManager.createIndex(Arrays.copyOf(docs, l), attribs,
					indexPaths[i]);
		}

		System.out.println(experimentNo + ". submitting queries..");
		int[][] foundCounter = new int[queryPartitionCount][dbPartitionCounts];
		int[][] lostCounter = new int[queryPartitionCount][dbPartitionCounts];
		for (int i = 0; i < queryPartitionCount; i++) {
			int endIndex = (int) ((i + 1.0) / queryPartitionCount * queriesList
					.size());
			List<FreebaseQuery> queries = queriesList.subList(0, endIndex);
			List<FreebaseQueryResult> oldResults = null;
			for (int j = 0; j < dbPartitionCounts; j++) {
				List<FreebaseQueryResult> queryResults = FreebaseDataManager
						.runFreebaseQueries(queries, indexPaths[j]);
				if (j == 0) {
					for (int k = 0; k < queryResults.size(); k++) {
						if (queryResults.get(k).p3() > 0)
							foundCounter[i][j]++;
					}
				} else {
					for (int k = 0; k < queryResults.size(); k++) {
						if (queryResults.get(k).p3() > oldResults.get(k).p3()) {
							foundCounter[i][j]++;
						} else if (queryResults.get(k).p3() < oldResults.get(k)
								.p3()) {
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

	// repeated randomized database size query size experiment
	// output: two files with rows as query set instances and columns as
	// database instances showing
	// average number of queries that gained and lost p@3
	public static void experiment_repeatRandomizedDatabaseSizeQuerySize() {
		ExperimentResult[] er = new ExperimentResult[50];
		ExperimentResult fr = new ExperimentResult();
		int qCount = 5;
		int dCount = 10;
		int expCount = 10;
		fr.lostCount = new int[qCount][dCount];
		fr.foundCount = new int[qCount][dCount];
		for (int i = 0; i < expCount; i++) {
			System.out.println("====== exp iteration " + i);
			er[i] = FreebaseExperiment
					.experiment_randomizedDatabaseSizeQuerySize(i, qCount,
							dCount, FreebaseExperiment.INDEX_BASE, "media");
			fr.lostCount = Utils.addMatrix(fr.lostCount, er[i].lostCount);
			fr.foundCount = Utils.addMatrix(fr.foundCount, er[i].foundCount);
		}
		FileWriter fw = null;
		try {
			fw = new FileWriter(FreebaseDataManager.resultDir
					+ "result_lost.csv");
			for (int i = 0; i < qCount; i++) {
				for (int j = 0; j < dCount - 1; j++) {
					fw.write(fr.lostCount[i][j] / ((double) expCount) + ",");
				}
				fw.write(fr.lostCount[i][dCount - 1] / ((double) expCount)
						+ "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			fw = new FileWriter(FreebaseDataManager.resultDir
					+ "result_found.csv");
			for (int i = 0; i < qCount; i++) {
				for (int j = 0; j < dCount - 1; j++) {
					fw.write(fr.foundCount[i][j] / ((double) expCount) + ",");
				}
				fw.write(fr.foundCount[i][dCount - 1] / ((double) expCount)
						+ "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}