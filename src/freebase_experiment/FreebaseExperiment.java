package freebase_experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;

public class FreebaseExperiment {
	
	public static String CLUSTER_FOLDER = "/scratch/cluster-share/ghadakcv/";
	public static String CLUSTER_INDEX = CLUSTER_FOLDER + "index/";
	public static String CLUSTER_RESULTS = "results/";

	public static void experiment_singleTable(String tableName, String[] attribs) {
		// runs database size experiment on media table
		// String attribs[] = { "name", "description" };
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesByRelevancyTable(tableName);
		String indexPath = FreebaseDataManager.INDEX_BASE + tableName + "/";
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

	public static void runExperiment2() {
		String tableName = "tbl_tv_program";
		String attribs[] = { FreebaseDataManager.NAME_ATTRIB,
				FreebaseDataManager.DESC_ATTRIB };
		String indexPath = FreebaseDataManager.INDEX_BASE + tableName + "/";
		// createIndex(tableName, attribs, indexPath);
		String pattern = "program| tv| television| serie| show | show$| film | film$| movie";
		String sql = "select * from query where text REGEXP '" + pattern
				+ "' and fbid in (select fbid from tbl_tv_program);";
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesBySqlQuery(sql);
		Pattern pat = Pattern.compile(pattern);
		for (FreebaseQuery query : queries) {
			System.out.println(query.text);
			Matcher matcher = pat.matcher(query.text.toLowerCase());
			matcher.find();
			String keyword = matcher.group(0);
			query.attribs.put(FreebaseDataManager.NAME_ATTRIB, query.text
					.toLowerCase().replace(keyword, ""));
			query.attribs.put(FreebaseDataManager.DESC_ATTRIB, query.text
					.toLowerCase().replace(keyword, ""));
		}
		try (FileWriter fw = new FileWriter(FreebaseDataManager.resultDir
				+ tableName + "_desc_name q_tvp.csv");) {
			for (FreebaseQuery query : queries) {
				FreebaseDataManager.runQuery(query, indexPath);
				fw.write(query.id + ", " + query.text + ", " + query.frequency
						+ ", " + query.wiki + ", " + query.p3() + ", "
						+ query.precisionAtK(20) + ", " + query.mrr() + ","
						+ query.hits[0] + ", " + query.hits[1] + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void runExperiment3() {
		String tableName = "tvp_film";
		String attribs[] = { FreebaseDataManager.NAME_ATTRIB,
				FreebaseDataManager.DESC_ATTRIB,
				FreebaseDataManager.SEMANTIC_TYPE_ATTRIB };
		String indexPath = FreebaseDataManager.INDEX_BASE + tableName + "/";
		// createIndex(tableName, attribs, indexPath);
		String pattern = "program| tv| television| serie| show | show$| film | film$| movie";
		String sql = "select * from query where text REGEXP '" + pattern
				+ "' and fbid in (select fbid from tbl_tv_program);";
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesBySqlQuery(sql);
		Pattern pat = Pattern.compile(pattern);
		for (FreebaseQuery query : queries) {
			System.out.println(query.text);
			Matcher matcher = pat.matcher(query.text.toLowerCase());
			matcher.find();
			String keyword = matcher.group(0);
			query.attribs
					.put(FreebaseDataManager.SEMANTIC_TYPE_ATTRIB, keyword);
			query.attribs.put(FreebaseDataManager.NAME_ATTRIB, query.text
					.toLowerCase().replace(keyword, ""));
			query.attribs.put(FreebaseDataManager.DESC_ATTRIB, query.text
					.toLowerCase().replace(keyword, ""));
		}
		try (FileWriter fw = new FileWriter(FreebaseDataManager.resultDir
				+ tableName + "_desc_name q_tvp.csv");) {
			for (FreebaseQuery query : queries) {
				FreebaseDataManager.runQuery(query, indexPath);
				fw.write(query.id + ", " + query.text + ", " + query.frequency
						+ ", " + query.wiki + ", " + query.p3() + ", "
						+ query.precisionAtK(20) + ", " + query.mrr() + ","
						+ query.hits[0] + ", " + query.hits[1] + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void experiment_keywordExtraction(String tableName,
			String pattern) {
		// String[] table = { "tbl_tv_program", "tbl_album", "tbl_book" };
		// String[] pattern = {
		// "program| tv| television| serie| show | show$| film | film$| movie",
		// "music|record|song|sound| art |album",
		// "book|theme|novel|notes|writing|manuscript|story" };

		String attribs[] = { FreebaseDataManager.NAME_ATTRIB,
				FreebaseDataManager.DESC_ATTRIB };
		String indexPath = FreebaseDataManager.INDEX_BASE + tableName + "/";
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

	public static void experiment_keywordExtraction(String tableName,
			String pattern, String queryTableName) {
		String attribs[] = { FreebaseDataManager.NAME_ATTRIB,
				FreebaseDataManager.DESC_ATTRIB,
				FreebaseDataManager.SEMANTIC_TYPE_ATTRIB };
		String indexPath = FreebaseDataManager.INDEX_BASE + tableName + "/";
		// createIndex(tableName, attribs, indexPath);
		String sql = "select * from query where text REGEXP '" + pattern
				+ "' and fbid in (select fbid from " + queryTableName + ");";
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesBySqlQuery(sql);
		FreebaseDataManager.extractAndRemoveKeyword(queries, pattern);
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

	public static void experiment_databaseSize() {
		// runs database size experiment on media table writing outputs to a
		// single file
		String tableName = "media";
		int mediaTableSize = 3285728;
		String attribs[] = { "name", "description" };
		System.out.println("Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesByRelevancyTable(tableName);
		String indexPaths[] = new String[10];
		for (int i = 0; i < 10; i++) {
			System.out.println("Building index " + i + "..");
			indexPaths[i] = FreebaseDataManager.INDEX_BASE + tableName + "_"
					+ i + "/";
		}
		System.out.println("submitting queries..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(FreebaseDataManager.resultDir + tableName
					+ "_mrr.csv");
			for (FreebaseQuery query : queries) {
				fw.write(query.id + ", " + query.text + ", " + query.frequency
						+ ", ");
				for (int i = 0; i < 10; i++) {
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

	public static void experiment_databaseSizeRandomized(int experimentNo) {
		// runs database size experiment on media table writing outputs to a
		// single file
		String tableName = "media";
		String attribs[] = { "name", "description" };
		System.out.println("Loading queries..");
		List<FreebaseQuery> queries = FreebaseDataManager
				.getQueriesByRelevancyTable(tableName);
		String indexPaths[] = new String[10];

		System.out.println("Loading tuples into docs..");
		String indexQuery = FreebaseDataManager.buildDataQuery(tableName,
				attribs);
		Document[] docs = FreebaseDataManager.loadTuplesToDocuments(indexQuery,
				attribs);
		shuffleArray(docs);
		int parts = 10;
		for (int i = 0; i < parts; i++) {
			System.out.println("Building index " + i + "..");
			indexPaths[i] = FreebaseDataManager.INDEX_BASE + tableName + "_"
					+ i + "/";
			int l = (int) (((i + 1.0) / parts) * docs.length);
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
				for (int i = 0; i < parts; i++) {
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

	static class ExperimentResult {
		int[][] lostCount;
		int[][] foundCount;
	}

	public static ExperimentResult experiment_randomizedDatabaseSizeQuerySize(
			int experimentNo, int queryPartitionCount, int dbPartitionCounts, String indexBase, String tableName) {
		// runs database size experiment on media table writing outputs to a
		// single file
		String attribs[] = { "name", "description" };

		System.out.println(experimentNo + ". Loading queries..");
		List<FreebaseQuery> queriesList = FreebaseDataManager
				.getQueriesByRelevancyTable(tableName);
		long seed = System.nanoTime();
		Collections.shuffle(queriesList, new Random(seed));

		System.out.println(experimentNo + ". Loading tuples into docs..");
		String indexPaths[] = new String[10];
		String indexQuery = FreebaseDataManager.buildDataQuery(tableName,
				attribs);
		Document[] docs = FreebaseDataManager.loadTuplesToDocuments(indexQuery,
				attribs);
		shuffleArray(docs);

		System.out.println(experimentNo + ".Building index..");
		for (int i = 0; i < dbPartitionCounts; i++) {
			indexPaths[i] = indexBase + experimentNo + "_" + tableName + "_"
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

	static void shuffleArray(Object[] ar) {
		// If running on Java 6 or older, use `new Random()` on RHS here
		Random rnd = new Random();
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			Object a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}

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
			er[i] = experiment_randomizedDatabaseSizeQuerySize(i, qCount,
					dCount, FreebaseDataManager.INDEX_BASE, "media");
			fr.lostCount = addMatrix(fr.lostCount, er[i].lostCount);
			fr.foundCount = addMatrix(fr.foundCount, er[i].foundCount);
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

	public static void experiment_randomizedDatabaseSizeQuerySizeOnCluster(
			int expNo, int qCount, int dCount, String tableName) {
		ExperimentResult fr = new ExperimentResult();
		fr.lostCount = new int[qCount][dCount];
		fr.foundCount = new int[qCount][dCount];
		fr = experiment_randomizedDatabaseSizeQuerySize(expNo, qCount, dCount, CLUSTER_INDEX, tableName);
		FileWriter fw = null;
		try {
			fw = new FileWriter(CLUSTER_RESULTS  + tableName + "_lost_" + expNo + ".csv");
			for (int i = 0; i < qCount; i++) {
				for (int j = 0; j < dCount - 1; j++) {
					fw.write(fr.lostCount[i][j] + ",");
				}
				fw.write(fr.lostCount[i][dCount - 1] + "\n");
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
			fw = new FileWriter(CLUSTER_RESULTS  + tableName + "_found_" + expNo + ".csv");
			for (int i = 0; i < qCount; i++) {
				for (int j = 0; j < dCount - 1; j++) {
					fw.write(fr.foundCount[i][j] + ",");
				}
				fw.write(fr.foundCount[i][dCount - 1] + "\n");
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

	public static int[][] addMatrix(int[][] a, int[][] b) {
		int[][] c = new int[a.length][a[0].length];
		for (int i = 0; i < a.length; i++) {
			for (int j = 0; j < a[0].length; j++) {
				c[i][j] = a[i][j] + b[i][j];
			}
		}
		return c;
	}

	public static void main(String[] args) {

		// experiment_repeatRandomizedDatabaseSizeQuerySize();
		int expNo = Integer.parseInt(args[0]);
		experiment_randomizedDatabaseSizeQuerySizeOnCluster(expNo, 5, 10, "media");

	}
}