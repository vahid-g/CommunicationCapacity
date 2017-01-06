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
import java.util.TreeSet;
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

    static class ExperimentConfig {
	String tableName = "tbl_all";
	String[] attribs = { "name", "description" };
	String name = "exp";
	double maxMrr = 0.5; // this is exclusive
	double trainSize = 1;
	double partitionSize = 0.3;

	String getFullName() {
	    return name + "_" + tableName + "_p" + partitionSize + "_h"
		    + maxMrr + ".csv";
	}

	String getIndexDir() {
	    return INDEX_BASE + tableName + "_p"
		    + (int) (partitionSize * 100) + "/";
	}
    }

    public static void main(String[] args) {

	ExperimentConfig config = new ExperimentConfig();

	// singleTable("tbl_all");
	// writeFreebaseQueryResults(resultList, config.getFullName() +
	// ".csv");

	// config.partitionCount = 2;
	// Map<FreebaseQueryInstance, List<FreebaseQueryResult>> results =
	// databaseSize(config);
	// writeFreebaseQueryResults(results, "exp10_" + config.tableName + "_h"
	// + config.hardness + "_ss" + (int)(config.trainSize * 100) + ".csv");

	List<FreebaseQueryResult> resultList = weightedSingleTablePartition(config);
	writeFreebaseQueryResults(resultList, config.getFullName());

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

    /**
     * Load tuple weights from the query log and runs the queries on a partition
     * of the table. Size of partition is decided based on config.
     * 
     * @param config
     * @return List of FreebaseQueryResult
     */
    public static List<FreebaseQueryResult> weightedSingleTablePartition(
	    ExperimentConfig config) {
	LOGGER.log(Level.INFO, "Loading queries..");
	List<FreebaseQuery> queries = FreebaseDataManager
		.loadMsnQueriesWithMaxMrr(config.maxMrr);
	// queries = Utils.flattenFreebaseQueries(queries);
	LOGGER.log(Level.INFO, "Loading tuples..");
	String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
		config.attribs);
	TreeMap<String, Integer> weights = FreebaseDataManager
		.loadQueryWeights(queries);
	// System.out.println(Collections.max(weights.values()));
	List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
		dataQuery, config.attribs, FreebaseDataManager.MAX_FETCH,
		weights);
	Collections.sort(docs, new DocumentFreqComparator());
	LOGGER.log(
		Level.INFO,
		"Highest weight: "
			+ docs.get(0).get(FreebaseDataManager.FREQ_ATTRIB));
	LOGGER.log(
		Level.INFO,
		"Least weight: "
			+ docs.get(docs.size() - 1).get(FreebaseDataManager.FREQ_ATTRIB));
	LOGGER.log(Level.INFO, "Building index " + "..");
	String indexPaths = INDEX_BASE + config.getFullName() + "/";
	FreebaseDataManager.createIndex(docs,
		(int) (config.partitionSize * docs.size()),
		config.attribs, indexPaths);
	LOGGER.log(Level.INFO, "Submitting Queries..");
	List<FreebaseQueryResult> fqrList = FreebaseDataManager
		.runFreebaseQueries(queries, indexPaths);
	return fqrList;
    }

    /**
     * Picks two samples of query log as train and test sets. Then runs the
     * weightedSingleTableSampled experiment using these sets. This experiment
     * loads the tuple weights based on train set, builds an index and runs the
     * test queries over the built index;
     * 
     * @param config
     * @return
     */
    public static List<FreebaseQueryResult> weightedSingleTablePartitionSampledQueries(
	    ExperimentConfig config) {
	int sampleSize = 3;
	LOGGER.log(Level.INFO, "Loading queries..");
	String sql = "select * from query_mrr_hard where mrr < "
		+ config.maxMrr + ";";
	List<FreebaseQuery> queries = FreebaseDataManager
		.loadMsnQueriesFromSql(sql);
	List<FreebaseQuery> trainQueries = Utils.sampleFreebaseQueries(queries,
		queries.size() / sampleSize);
	List<FreebaseQuery> testQueries = Utils.sampleFreebaseQueries(queries,
		queries.size() / sampleSize);
	return weightedSingleTablePartitionSampledQueries(config, trainQueries,
		testQueries);
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
    public static List<FreebaseQueryResult> weightedSingleTablePartitionSampledQueries(
	    ExperimentConfig config, List<FreebaseQuery> trainQueries,
	    List<FreebaseQuery> testQueries) {
	LOGGER.log(Level.INFO, "Loading tuples..");
	String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
		config.attribs);
	TreeMap<String, Integer> weights = FreebaseDataManager
		.loadQueryWeights(trainQueries);
	List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
		dataQuery, config.attribs, FreebaseDataManager.MAX_FETCH,
		weights);
	Collections.shuffle(docs);
	Collections.sort(docs, new DocumentFreqComparator());
	LOGGER.log(Level.INFO, "Building index " + "..");
	String indexPaths = INDEX_BASE + config.getFullName() + "/";
	FreebaseDataManager.createIndex(docs,
		(int) (config.partitionSize * docs.size()),
		config.attribs, indexPaths);
	LOGGER.log(Level.INFO, "Submitting Queries..");
	List<FreebaseQueryResult> fqrList = FreebaseDataManager
		.runFreebaseQueries(testQueries, indexPaths);
	return fqrList;
    }

    /**
     * 
     * Selects a partition of database based on the partitionPercentage config
     * param and builds index on it. This partition is based on equal
     * percentages of relevant and non-relevant tuples. Then runs test queries
     * this indexed partition. Note that this method considers weight for tuples
     * (deduced based on weights of queries).
     * 
     * @param config
     * @return
     */
    public static List<FreebaseQueryResult> weightedSingleTableMixedPartitionSampledQueries(
	    ExperimentConfig config) {
	int sampleSize = 3;
	LOGGER.log(Level.INFO, "Loading queries..");
	String sql = "select * from query_mrr_hard where mrr < "
		+ config.maxMrr + ";";
	List<FreebaseQuery> queries = FreebaseDataManager
		.loadMsnQueriesFromSql(sql);
	List<FreebaseQuery> trainQueries = Utils.sampleFreebaseQueries(queries,
		queries.size() / sampleSize);
	List<FreebaseQuery> testQueries = Utils.sampleFreebaseQueries(queries,
		queries.size() / sampleSize);
	return weightedSingleTableMixedPartitionSampledQueries(config,
		trainQueries, testQueries);
    }

    public static List<FreebaseQueryResult> weightedSingleTableMixedPartitionSampledQueries(
	    ExperimentConfig config, List<FreebaseQuery> trainQueries,
	    List<FreebaseQuery> testQueries) {
	LOGGER.log(Level.INFO, "Loading tuples..");
	String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
		config.attribs);
	TreeMap<String, Integer> weights = FreebaseDataManager
		.loadQueryWeights(trainQueries);
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
	Collections.sort(docs, new DocumentFreqComparator());
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
		(int) (config.partitionSize * nonRels.size()),
		config.attribs, indexPaths);
	FreebaseDataManager.appendIndex(rels,
		(int) (config.partitionSize * rels.size()),
		config.attribs, indexPaths);
	LOGGER.log(Level.INFO, "Submitting Queries..");
	List<FreebaseQueryResult> fqrList = FreebaseDataManager
		.runFreebaseQueries(testQueries, indexPaths);
	return fqrList;
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
	String sql = "select * from query_mrr_hard where mrr < "
		+ config.maxMrr + ";";
	List<FreebaseQuery> queryList = FreebaseDataManager
		.loadMsnQueriesFromSql(sql);
	List<FreebaseQuery> flatQueryList = Utils
		.flattenFreebaseQueries(queryList);
	// random sampling
	Collections.shuffle(flatQueryList);
	List<FreebaseQuery> trainQueries = flatQueryList.subList(0,
		(int) (flatQueryList.size() * config.trainSize));
	// List<FreebaseQueryInstance> testQueries = new
	// ArrayList<FreebaseQueryInstance>();
	// testQueries.addAll(flatQueryList);
	// testQueries.removeAll(trainQueries);
	List<FreebaseQuery> testQueries = trainQueries;
	LOGGER.log(Level.INFO, "train size: " + trainQueries.size());
	LOGGER.log(Level.INFO, "test size: " + testQueries.size());
	LOGGER.log(Level.INFO, "Loading tuples..");
	String dataQuery = FreebaseDataManager.buildDataQuery(config.tableName,
		config.attribs);
	TreeMap<String, Integer> weights = FreebaseDataManager
		.loadQueryInstanceWeights(trainQueries);
	List<Document> docs = FreebaseDataManager.loadTuplesToDocuments(
		dataQuery, config.attribs, FreebaseDataManager.MAX_FETCH,
		weights);
	Collections.shuffle(docs);
	Collections.sort(docs, new DocumentFreqComparator());

	String indexPaths[] = new String[partitionCount];
	Map<FreebaseQuery, List<FreebaseQueryResult>> results = new HashMap<FreebaseQuery, List<FreebaseQueryResult>>();
	for (FreebaseQuery query : testQueries) {
	    results.put(query, new ArrayList<FreebaseQueryResult>());
	}
	for (int i = 0; i < partitionCount; i++) {
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
	String sql = "select * from query_mrr_hard where mrr < "
		+ config.maxMrr + ";";
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