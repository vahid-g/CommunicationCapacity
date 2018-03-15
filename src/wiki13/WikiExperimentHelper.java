package wiki13;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.search.similarities.BM25Similarity;

import indexing.InexDatasetIndexer;
import indexing.InexFile;
import query.ExperimentQuery;
import query.LuceneQueryBuilder;
import query.QueryResult;
import query.QueryServices;

public class WikiExperimentHelper {

	private static final Logger LOGGER = Logger.getLogger(WikiExperimentHelper.class.getName());

	public static void main(String[] args) {

	}

	public static void buildGlobalIndex(int expNo, int totalExp, String accessCountsFilePath, String indexPath) {
		try {
			List<InexFile> pathCountList = InexFile.loadInexFileList(accessCountsFilePath);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList(0, (int) (((double) expNo / total) * pathCountList.size()));
			LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
			LOGGER.log(Level.INFO, "Smallest score: " + pathCountList.get(pathCountList.size() - 1).weight);
			buildGlobalIndex(pathCountList, indexPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void buildComplementIndex(int expNo, int totalExp, String accessCountsFilePath, String indexPath) {
		try {
			List<InexFile> pathCountList = InexFile.loadInexFileList(accessCountsFilePath);
			double total = (double) totalExp;
			pathCountList = pathCountList.subList((int) (((double) expNo / total) * pathCountList.size()),
					pathCountList.size());
			LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
			LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
			LOGGER.log(Level.INFO, "Smallest score: " + pathCountList.get(pathCountList.size() - 1).weight);
			buildGlobalIndex(pathCountList, indexPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static void buildGlobalIndex(List<InexFile> files, String indexPath) {
		try {
			File indexPathFile = new File(indexPath);
			if (!indexPathFile.exists()) {
				indexPathFile.mkdirs();
			}
			LOGGER.log(Level.INFO, "Building index at: " + indexPath);
			WikiFileIndexer fileIndexer = new WikiFileIndexer();
			InexDatasetIndexer idi = new InexDatasetIndexer(fileIndexer);
			idi.buildIndex(files, indexPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static List<QueryResult> runQueriesOnGlobalIndex(String indexPath, List<ExperimentQuery> queries,
			float gamma, boolean boostDoc) {
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		fieldToBoost.put(WikiFileIndexer.TITLE_ATTRIB, gamma);
		fieldToBoost.put(WikiFileIndexer.CONTENT_ATTRIB, 1 - gamma);
		LuceneQueryBuilder lqb;
		if (boostDoc) {
			lqb = new LuceneQueryBuilder(fieldToBoost, WikiFileIndexer.WEIGHT_ATTRIB);
		} else {
			lqb = new LuceneQueryBuilder(fieldToBoost);
		}
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(queries, indexPath, new BM25Similarity(), lqb,
				false);
		return results;
	}

	public static List<QueryResult> runQueriesOnGlobalIndex(String indexPath, List<ExperimentQuery> queries,
			float gamma) {
		return runQueriesOnGlobalIndex(indexPath, queries, gamma, false);
	}

	public static void writeQueryResultsToFile(List<QueryResult> results, String resultDirPath, String resultFileName) {
		LOGGER.log(Level.INFO, "Writing results..");
		File resultDir = new File(resultDirPath);
		if (!resultDir.exists()) {
			resultDir.mkdirs();
		}
		try (FileWriter fw = new FileWriter(resultDirPath + resultFileName)) {
			for (QueryResult iqr : results) {
				fw.write(iqr.resultString() + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public static void writeMapToFile(Map<String, Double> map, String filename) {
		try (FileWriter fw = new FileWriter(filename)) {
			for (String s : map.keySet()) {
				fw.write(s + "," + map.get(s) + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

}
