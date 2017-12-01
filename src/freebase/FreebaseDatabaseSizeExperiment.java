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

    static final String TBL_NAME = "tbl_all";
    static final int TBL_SIZE = 6046089;
    static final String[] ATTRIBS = { "name", "description" };
    static final String queryTableName = "query_all_mrr_weighted";

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

    public static final class DocumentFreqComparator implements
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

    public static void main(String[] args) {
	// // for finding hard queries
	// List<FreebaseQueryResult> fqrList = weightedTablePartition();
	// writeFreebaseQueryResults(fqrList, "tbl_all_weighted_mrr.csv");

	// for computing optimal database size
	// Map<FreebaseQuery, List<FreebaseQueryResult>> fqrMap =
	// databaseSizeOptimalFlat(10);
	// Map<FreebaseQuery, List<FreebaseQueryResult>> fqrMap =
	// databaseSize(10,
	// 0.5);

	// Map<FreebaseQuery, List<FreebaseQueryResult>> fqrMap =
	// databaseSize(10,
	// 0.8);
	// writeFreebaseQueryResults(fqrMap,
	// "tbl_all_trained_alpha1_train80.csv");

	generateDistributions(0.8);
    }

    /**
     * runs all queries from "query" table on a single table instance
     * 
     * @param tableName
     * @param ATTRIBS
     */
    public static List<FreebaseQueryResult> singleTable() {
	List<FreebaseQuery> queries = FreebaseDataManager
		.loadMsnQueries("query_all");
	String indexPath = INDEX_BASE;
	String dataQuery = FreebaseDataManager
		.buildDataQuery(TBL_NAME, ATTRIBS);
	List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
		dataQuery, ATTRIBS, FreebaseDataManager.MAX_FETCH);
	FreebaseDataManager.createIndex(docs, ATTRIBS, indexPath);
	List<FreebaseQueryResult> fqrList = FreebaseDataManager
		.runFreebaseQueries(queries, indexPath);
	return fqrList;
    }

    /**
     * Load tuple weights from the query log and runs the queries on a partition
     * of the table. Size of partition is decided based on config.
     * 
     * @param config
     * @return List of FreebaseQueryResult
     */
    public static List<FreebaseQueryResult> weightedTablePartition() {
	LOGGER.log(Level.INFO, "Loading queries..");
	List<FreebaseQuery> queries = FreebaseDataManager
		.loadMsnQueries("query_all");
	// List<FreebaseQuery> queries = FreebaseDataManager
	// .loadMsnQueriesWithMaxMrr(queryTableName, 1);
	return weightedTablePartition(queries, queries, 1);
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
	    List<FreebaseQuery> trainQueries, List<FreebaseQuery> testQueries,
	    int partitionSize) {
	LOGGER.log(Level.INFO, "Loading tuples..");
	String dataQuery = FreebaseDataManager
		.buildDataQuery(TBL_NAME, ATTRIBS);
	TreeMap<String, Float> weights = FreebaseDataManager
		.loadQueryWeights(trainQueries);
	List<Document> docs = FreebaseDataManager
		.loadTuplesToDocumentsSmoothing(dataQuery, ATTRIBS,
			FreebaseDataManager.MAX_FETCH, weights, 1, TBL_SIZE);
	// Collections.shuffle(docs);
	Collections.sort(docs, new DocumentFreqComparator());
	LOGGER.log(Level.INFO, "Building index " + "..");
	String indexPaths = INDEX_BASE;
	FreebaseDataManager.createIndex(docs,
		(int) (partitionSize * docs.size()), ATTRIBS, indexPaths);
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
	    int partitionCount) {
	LOGGER.log(Level.INFO, "Loading queries..");
	List<FreebaseQuery> queryList = FreebaseDataManager
		.loadMsnQueriesWithMaxMrr(queryTableName, 1);
	TreeMap<String, Float> weights = FreebaseDataManager
		.loadQueryWeights(queryList);
	String dataQuery = FreebaseDataManager
		.buildDataQuery(TBL_NAME, ATTRIBS);
	List<Document> docs = FreebaseDataManager
		.loadTuplesToDocumentsSmoothing(dataQuery, ATTRIBS,
			FreebaseDataManager.MAX_FETCH, weights, 1.0, TBL_SIZE);
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
	    indexPaths[i] = INDEX_BASE + TBL_NAME + "_" + i + "/";
	    FreebaseDataManager.createIndex(docs,
		    (int) (((i + 1.0) / partitionCount) * docs.size()),
		    ATTRIBS, indexPaths[i]);
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
     * database size experiment on a single table
     * 
     * @param tableName
     * @return a list of FreebaseQueryResults objects.
     */
    public static Map<FreebaseQuery, List<FreebaseQueryResult>> databaseSizeOptimalFlat(
	    int partitionCount) {
	LOGGER.log(Level.INFO, "Loading queries..");
	List<FreebaseQuery> queryList = Utils
		.flattenFreebaseQueries(FreebaseDataManager
			.loadMsnQueriesWithMaxMrr(queryTableName, 1));
	TreeMap<String, Float> weights = FreebaseDataManager
		.loadQueryInstanceWeights(queryList);
	String dataQuery = FreebaseDataManager
		.buildDataQuery(TBL_NAME, ATTRIBS);
	List<Document> docs = FreebaseDataManager
		.loadTuplesToDocumentsSmoothing(dataQuery, ATTRIBS,
			FreebaseDataManager.MAX_FETCH, weights, 1.0, TBL_SIZE);
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
	    indexPaths[i] = INDEX_BASE + TBL_NAME + "_" + i + "/";
	    FreebaseDataManager.createIndex(docs,
		    (int) (((i + 1.0) / partitionCount) * docs.size()),
		    ATTRIBS, indexPaths[i]);
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
    /**
     * @param partitionCount
     * @param trainSize
     * @return
     */
    public static Map<FreebaseQuery, List<FreebaseQueryResult>> databaseSize(
	    int partitionCount, double trainSize) {
	LOGGER.log(Level.INFO, "Loading queries..");
	List<FreebaseQuery> queryList = FreebaseDataManager
		.loadMsnQueriesWithMaxMrr(queryTableName, 1);
	List<FreebaseQuery> flatQueryList = Utils
		.flattenFreebaseQueries(queryList);
	// random sampling
	Collections.shuffle(flatQueryList);
	List<FreebaseQuery> trainQueries = flatQueryList.subList(0,
		(int) (flatQueryList.size() * trainSize));
	List<FreebaseQuery> testQueries = new ArrayList<FreebaseQuery>();
	testQueries.addAll(flatQueryList);
	testQueries.removeAll(trainQueries);
	LOGGER.log(Level.INFO, "train size: " + trainQueries.size());
	LOGGER.log(Level.INFO, "test size: " + testQueries.size());
	LOGGER.log(Level.INFO, "Loading tuples..");
	String dataQuery = FreebaseDataManager
		.buildDataQuery(TBL_NAME, ATTRIBS);
	TreeMap<String, Float> weights = FreebaseDataManager
		.loadQueryInstanceWeights(trainQueries);
	List<Document> docs = FreebaseDataManager
		.loadTuplesToDocumentsSmoothing(dataQuery, ATTRIBS,
			FreebaseDataManager.MAX_FETCH, weights, 1, TBL_SIZE);
	Collections.shuffle(docs);
	Collections.sort(docs, new DocumentFreqComparator());

	String indexPaths[] = new String[partitionCount];
	Map<FreebaseQuery, List<FreebaseQueryResult>> results = new HashMap<FreebaseQuery, List<FreebaseQueryResult>>();
	for (FreebaseQuery query : testQueries) {
	    results.put(query, new ArrayList<FreebaseQueryResult>());
	}
	for (int i = 0; i < partitionCount; i++) {
	    LOGGER.log(Level.INFO, "Building index " + i + "..");
	    indexPaths[i] = INDEX_BASE + TBL_NAME + "_" + i + "/";
	    FreebaseDataManager.createIndex(docs,
		    (int) (((i + 1.0) / (partitionCount)) * docs.size()),
		    ATTRIBS, indexPaths[i]);
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

    public static void generateDistributions(double trainSize) {
	LOGGER.log(Level.INFO, "Loading queries..");
	List<FreebaseQuery> queryList = FreebaseDataManager
		.loadMsnQueriesWithMaxMrr(queryTableName, 1);
	List<FreebaseQuery> flatQueryList = Utils
		.flattenFreebaseQueries(queryList);
	// random sampling
	Collections.shuffle(flatQueryList);
	List<FreebaseQuery> trainQueries = flatQueryList.subList(0,
		(int) (flatQueryList.size() * trainSize));
	List<FreebaseQuery> testQueries = new ArrayList<FreebaseQuery>();
	testQueries.addAll(flatQueryList);
	testQueries.removeAll(trainQueries);
	System.out.println("train size: " + trainQueries.size());
	System.out.println("test size: " + testQueries.size());
	// generating distributions
	HashMap<String, Integer> cuntMap = new HashMap<String, Integer>();
	for (FreebaseQuery query : trainQueries) {
	    if (cuntMap.containsKey(query.text)) {
		cuntMap.put(query.text, cuntMap.get(query.text) + 1);
	    } else {
		cuntMap.put(query.text, 1);
	    }
	}
	HashMap<String, Integer> cuntMap2 = new HashMap<String, Integer>();
	for (FreebaseQuery query : testQueries) {
	    if (cuntMap2.containsKey(query.text)) {
		cuntMap2.put(query.text, cuntMap2.get(query.text) + 1);
	    } else {
		cuntMap2.put(query.text, 1);
	    }
	}
	FileWriter fw = null;
	try {
	    fw = new FileWriter(RESULT_DIR + "train_cunts.csv");
	    for (String key : cuntMap.keySet())
		fw.write(key + ", " + cuntMap.get(key) + "\n");
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
	try {
	    fw = new FileWriter(RESULT_DIR + "test_cunts.csv");
	    for (String key : cuntMap2.keySet())
		fw.write(key + ", " + cuntMap2.get(key) + ", "
			+ cuntMap.get(key) + "\n");
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
		for (FreebaseQueryResult fqr : list) {
		    fw.write(", " + fqr.p3());
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