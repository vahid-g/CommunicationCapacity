package wiki13;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.search.similarities.BM25Similarity;

import indexing.InexFile;
import popularity.PopularityUtils;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;

public class WikiMapleExperiment {

	private static final Logger LOGGER = Logger
			.getLogger(WikiMapleExperiment.class.getName());
	private static final String DATA_PATH = "/data/ghadakcv/";
	private static final String INDEX_PATH = DATA_PATH + "wiki_index";
	private static final String FILELIST_PATH = DATA_PATH
			+ "wiki13_count13_text.csv";
	private static final String QUERY_FILE_PATH = DATA_PATH + "2013-adhoc.xml";
	private static final String QREL_FILE_PATH = DATA_PATH + "2013-adhoc.qrels";

	public static void main(String[] args) {
		if (args.length < 1) {
			LOGGER.log(Level.SEVERE,
					"A flag should be specified. Available flags are: \n\t--index\n\t--query-X\n");
		} else if (args[0].equals("--index")) {
			buildIndex(FILELIST_PATH, INDEX_PATH);
		} else if (args[0].equals("--query-1")) {
			List<QueryResult> results = runQueriesOnGlobalIndex(INDEX_PATH,
					QUERY_FILE_PATH, QREL_FILE_PATH);
			Map<String, Double> idPopMap = PopularityUtils
					.loadIdPopularityMap(FILELIST_PATH);
			filterResultsWithQueryThreshold(results, idPopMap, "./");
		} else if (args[0].equals("--query-2")) {
			List<InexFile> inexFiles = InexFile.loadInexFileList(FILELIST_PATH);
			List<QueryResult> results = runQueriesOnGlobalIndex(INDEX_PATH,
					QUERY_FILE_PATH, QREL_FILE_PATH);
			Map<String, Double> idPopMap = PopularityUtils
					.loadIdPopularityMap(FILELIST_PATH);
			filterResultsWithDatabaseThreshold(results, idPopMap, "./",
					inexFiles);
		} else {
			LOGGER.log(Level.SEVERE,
					"Flag not recognized. Available flags are: \n\t--index\n\t--query\n");
		}
	}

	private static void buildIndex(String fileListPath,
			String indexDirectoryPath) {
		try {
			List<InexFile> pathCountList = InexFile
					.loadInexFileList(fileListPath);
			LOGGER.log(Level.INFO, "Number of loaded path_counts: "
					+ pathCountList.size());
			File indexPathFile = new File(indexDirectoryPath);
			if (!indexPathFile.exists()) {
				indexPathFile.mkdirs();
			}
			LOGGER.log(Level.INFO, "Building index at: " + indexDirectoryPath);
			Wiki13Indexer.buildIndexOnText(pathCountList, indexDirectoryPath,
					new BM25Similarity());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<QueryResult> runQueriesOnGlobalIndex(String indexPath,
			String queriesFilePath, String qrelsFilePath) {
		LOGGER.log(Level.INFO, "Loading queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				queriesFilePath, qrelsFilePath);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		fieldToBoost.put(Wiki13Indexer.TITLE_ATTRIB, 0.15f);
		fieldToBoost.put(Wiki13Indexer.CONTENT_ATTRIB, 0.85f);
		LOGGER.log(Level.INFO, "Running queries..");
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(
				queries, indexPath, new BM25Similarity(), fieldToBoost);
		return results;
	}

	static void filterResultsWithQueryThreshold(List<QueryResult> results,
			Map<String, Double> idPopMap, String resultDirectoryPath) {
		LOGGER.log(Level.INFO, "Caching results..");
		QueryResult newResult;
		try (FileWriter p20Writer = new FileWriter("wiki_p20.csv");
				FileWriter mrrWriter = new FileWriter("wiki_mrr.csv");
				FileWriter rec200Writer = new FileWriter(resultDirectoryPath
						+ "wiki_recall200.csv");
				FileWriter recallWriter = new FileWriter("wiki_recall.csv")) {
			for (QueryResult result : results) {
				p20Writer.write(result.query.getText());
				mrrWriter.write(result.query.getText());
				rec200Writer.write(result.query.getText());
				recallWriter.write(result.query.getText());
				for (double x = 0.01; x <= 1; x += 0.01) {
					double threshold = findThresholdPerQuery(result, idPopMap,
							x);
					newResult = filterQueryResult(result, idPopMap, threshold);
					p20Writer.write("," + newResult.precisionAtK(20));
					mrrWriter.write("," + newResult.mrr());
					rec200Writer.write("," + newResult.mrr());
					recallWriter.write("," + newResult.recallAtK(1000));
				}
				p20Writer.write("\n");
				mrrWriter.write("\n");
				rec200Writer.write("\n");
				recallWriter.write("\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	static void filterResultsWithDatabaseThreshold(List<QueryResult> results,
			Map<String, Double> idPopMap, String resultDirectoryPath,
			List<InexFile> inexFiles) {
		LOGGER.log(Level.INFO, "Caching results..");
		QueryResult newResult;
		try (FileWriter p20Writer = new FileWriter(resultDirectoryPath
				+ "wiki_p20.csv");
				FileWriter mrrWriter = new FileWriter(resultDirectoryPath
						+ "wiki_mrr.csv");
				FileWriter rec200Writer = new FileWriter(resultDirectoryPath
						+ "wiki_recall200.csv");
				FileWriter recallWriter = new FileWriter(resultDirectoryPath
						+ "wiki_recall.csv")) {
			for (QueryResult result : results) {
				p20Writer.write(result.query.getText());
				mrrWriter.write(result.query.getText());
				rec200Writer.write(result.query.getText());
				recallWriter.write(result.query.getText());
				for (double x = 0.01; x <= 1; x += 0.01) {
					double threshold = findThresholdPerDatabase(inexFiles, x);
					newResult = filterQueryResult(result, idPopMap, threshold);
					p20Writer.write("," + newResult.precisionAtK(20));
					mrrWriter.write("," + newResult.mrr());
					rec200Writer.write("," + newResult.mrr());
					recallWriter.write("," + newResult.recallAtK(1000));
				}
				p20Writer.write("\n");
				mrrWriter.write("\n");
				rec200Writer.write("\n");
				recallWriter.write("\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private static double findThresholdPerQuery(QueryResult result,
			Map<String, Double> idPopMap, double cutoffSize) {
		List<Double> pops = new ArrayList<Double>();
		for (String id : result.getTopResults()) {
			pops.add(idPopMap.get(id));
		}
		Collections.sort(pops, Collections.reverseOrder());
		double cutoffWeight = pops.get((int) Math.floor(cutoffSize
				* pops.size()) - 1);
		return cutoffWeight;
	}

	private static double findThresholdPerDatabase(List<InexFile> inexFiles,
			double cutoffSize) {
		// TODO: what if cutoff == 0
		int lastItem = (int) Math.floor(cutoffSize * inexFiles.size() - 1);
		return inexFiles.get(lastItem).weight;
	}

	private static QueryResult filterQueryResult(QueryResult result,
			Map<String, Double> idPopMap, double cutoffWeight) {
		QueryResult newResult = new QueryResult(result.query);
		if (result.getTopResults().size() < 2) {
			LOGGER.log(Level.WARNING, "query just has zero or one result");
			return newResult;
		}
		List<String> newTopResults = new ArrayList<String>();
		List<String> newTopResultTitles = new ArrayList<String>();
		for (int i = 0; i < result.getTopResults().size(); i++) {
			if (idPopMap.get(result.getTopResults().get(i)) >= cutoffWeight) {
				newTopResults.add(result.getTopResults().get(i));
				newTopResultTitles.add(result.getTopResultsTitle().get(i));
			}
		}
		newResult.setTopResults(newTopResults);
		newResult.setTopResultsTitle(newTopResultTitles);
		return newResult;
	}

	public static void writeResultsToFile(List<QueryResult> results,
			String resultFilePath) {
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(resultFilePath)) {
			for (QueryResult iqr : results) {
				fw.write(iqr.resultString() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
