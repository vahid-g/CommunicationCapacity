package amazon;

import indexing.InexFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.search.similarities.BM25Similarity;

import query.ExperimentQuery;
import query.Qrel;
import query.QueryResult;
import query.QueryServices;
import amazon.datatools.AmazonDeweyConverter;
import amazon.datatools.AmazonIsbnConverter;
import amazon.indexing.AmazonDatasetIndexer;
import amazon.indexing.AmazonIndexer;

public class AmazonExperiment {

	private static final Logger LOGGER = Logger
			.getLogger(AmazonExperiment.class.getName());

	private AmazonDocumentField[] fields = {AmazonDocumentField.TITLE,
			AmazonDocumentField.CONTENT, AmazonDocumentField.CREATORS,
			AmazonDocumentField.TAGS, AmazonDocumentField.DEWEY};
	private String[] queryFields = {"mediated_query", "title", "group",
			"narrative"};
	private int expNo;
	private int total;
	private String expName;
	private String indexPath;
	private String isbnsFilePath;

	public AmazonExperiment(int experimentNumber, int partitionCount,
			String experimentSuffix, String isbnsFilePath) {
		this.expNo = experimentNumber;
		this.total = partitionCount;
		expName = "amazon_p" + partitionCount + "_bm_f" + fields.length + "_"
				+ experimentSuffix;
		indexPath = AmazonDirectoryInfo.GLOBAL_INDEX_DIR + expName + "/"
				+ expNo;
		this.isbnsFilePath = isbnsFilePath;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			LOGGER.log(Level.SEVERE,
					"Input argument is missing. Terminating the program..");
			return;
		}
		int expNo = Integer.parseInt(args[0]);
		int totalPartitionNo = Integer.parseInt(args[1]);
		AmazonExperiment experiment = new AmazonExperiment(expNo,
				totalPartitionNo, "ratecount", AmazonDirectoryInfo.HOME
						+ "data/path_counts/amazon_path_ratecount.csv");
		// experiment.buildGlobalIndex();
		// Map<String, Float> fieldBoostMap =
		// experiment.gridSearchOnGlobalIndex(AmazonDirectoryInfo.TEST_QUERY_FILE,
		// AmazonDirectoryInfo.QREL_FILE, experiment.queryFields);
		Map<String, Float> fieldBoostMap = new HashMap<String, Float>();
		fieldBoostMap.put(AmazonDocumentField.TITLE.toString(), 0.22f);
		fieldBoostMap.put(AmazonDocumentField.CONTENT.toString(), 0.62f);
		fieldBoostMap.put(AmazonDocumentField.CREATORS.toString(), 0.04f);
		fieldBoostMap.put(AmazonDocumentField.TAGS.toString(), 0.1f);
		fieldBoostMap.put(AmazonDocumentField.DEWEY.toString(), 0.02f);
		experiment.expOnGlobalIndex(fieldBoostMap,
				AmazonDirectoryInfo.QUERY_FILE,
				// + "data/queries/amazon/ml_test_topics.xml",
				AmazonDirectoryInfo.QREL_FILE, experiment.queryFields);
	}

	void buildGlobalIndex() {
		List<InexFile> fileList = InexFile.loadInexFileList(this.isbnsFilePath);
		LOGGER.log(Level.INFO, "Building index..");
		fileList = fileList.subList(0, (fileList.size() * expNo) / total);
		AmazonIndexer fileIndexer = new AmazonIndexer(fields,
				AmazonIsbnConverter
						.loadIsbnToLtidMap(AmazonDirectoryInfo.ISBN_DICT),
				AmazonDeweyConverter
						.getInstance(AmazonDirectoryInfo.DEWEY_DICT));
		AmazonDatasetIndexer datasetIndexer = new AmazonDatasetIndexer(
				fileIndexer);
		datasetIndexer.buildIndex(fileList, indexPath);
	}

	public Map<String, Float> gridSearchOnGlobalIndex(String queryFile,
			String qrelFile, String[] queryFields) {
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				queryFile, qrelFile, queryFields);
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		int fieldCount = fields.length;
		float[] p10 = new float[fieldCount];
		float[] mrr = new float[fieldCount];
		float[] map = new float[fieldCount];
		float[] all = new float[fieldCount];
		for (int i = 0; i < fieldCount; i++) {
			Map<String, Float> fieldToBoost = new HashMap<String, Float>();
			for (AmazonDocumentField field : fields)
				fieldToBoost.put(field.toString(), 0f);
			fieldToBoost.put(fields[i].toString(), 1f);
			LOGGER.log(Level.INFO, "Field as doc result with " + fields[i]
					+ " : " + fieldToBoost.toString());
			List<QueryResult> results = QueryServices.runQueriesWithBoosting(
					queries, indexPath, new BM25Similarity(), fieldToBoost);
			convertIsbnToLtidAndFilter(results);
			for (QueryResult queryResult : results) {
				p10[i] += queryResult.precisionAtK(10);
				mrr[i] += queryResult.mrr();
				map[i] += queryResult.averagePrecision();
			}
			p10[i] /= results.size();
			mrr[i] /= results.size();
			map[i] /= results.size();
		}
		float p10Sum = 0;
		float mrrSum = 0;
		float mapSum = 0;
		for (int i = 0; i < fieldCount; i++) {
			p10Sum += p10[i];
			mrrSum += mrr[i];
			mapSum += map[i];
		}
		for (int i = 0; i < fieldCount; i++) {
			p10[i] /= p10Sum;
			mrr[i] /= mrrSum;
			map[i] /= mapSum;
			all[i] = (p10[i] + mrr[i] + map[i]) / 3.0f;
		}
		LOGGER.log(Level.INFO,
				"Results of field as a document retrieval (best p10): "
						+ Arrays.toString(p10));
		LOGGER.log(Level.INFO,
				"Results of field as a document retrieval (best mrr): "
						+ Arrays.toString(mrr));
		LOGGER.log(Level.INFO,
				"Results of field as a document retrieval (best map): "
						+ Arrays.toString(map));
		LOGGER.log(Level.INFO,
				"Results of field as a document retrieval (best all): "
						+ Arrays.toString(all));
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		for (int i = 0; i < fieldCount; i++) {
			fieldToBoost.put(fields[i].toString(), all[i]);
		}
		return fieldToBoost;
	}

	private void expOnGlobalIndex(Map<String, Float> fieldToBoost,
			String queryFile, String qrelFile, String[] queryFields) {
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				queryFile, qrelFile, queryFields);
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(
				queries, indexPath, new BM25Similarity(), fieldToBoost);
		LOGGER.log(Level.INFO, "updating ISBN results to LTID..");
		convertIsbnToLtidAndFilter(results);
		LOGGER.log(Level.INFO, "Writing results to file..");
		File resultDir = new File(AmazonDirectoryInfo.RESULT_DIR + expName);
		resultDir.mkdirs();
		// Map<String, Set<String>> ltidToIsbns = AmazonIsbnConverter
		// .loadLtidToIsbnMap(AmazonDirectoryInfo.ISBN_DICT);
		try (FileWriter fw = new FileWriter(resultDir.getAbsolutePath() + "/"
				+ expNo + ".csv")
		// ;FileWriter fw2 = new FileWriter(
		// AmazonDirectoryInfo.RESULT_DIR + "amazon_" + expNo + ".log")
		) {
			for (QueryResult mqr : results) {
				fw.write(mqr.resultString() + "\n");
				// fw2.write(generateLog(mqr, ltidToIsbns) + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage());
		}
	}

	String generateLog(QueryResult queryResult,
			Map<String, Set<String>> ltidToIsbn) {
		ExperimentQuery query = queryResult.query;
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + query.getId() + "\t" + queryResult.mrr() + "\n");
		sb.append("query: " + query.getText() + "\n\n");
		sb.append("|relevant tuples| = " + query.getQrels().size() + "\n");
		sb.append("|returned results| = " + queryResult.getTopResults().size()
				+ "\n");
		int counter = 0;
		sb.append("returned results: \n");
		AmazonIsbnWeightMap aiwm = AmazonIsbnWeightMap
				.getInstance(isbnsFilePath);
		for (int i = 0; i < queryResult.getTopResults().size(); i++) {
			String returnedLtid = queryResult.getTopResults().get(i);
			String returnedTitle = queryResult.getTopResultsTitle().get(i);
			String isbn = returnedTitle
					.substring(0, returnedTitle.indexOf(':'));
			if (query.hasQrelId(returnedLtid)) {
				sb.append("++ " + returnedLtid + "\t" + returnedTitle + "\t"
						+ aiwm.getWeight(isbn) + "\n");
			} else {
				sb.append("-- " + returnedLtid + "\t" + returnedTitle + "\t"
						+ aiwm.getWeight(isbn) + "\n");
			}
			if (counter++ > 10)
				break;
		}
		counter = 0;
		sb.append("missed docs: ");
		for (Qrel qrel : query.getQrels()) {
			String relevantLtid = qrel.getQrelId();
			if (!queryResult.getTopResults().contains(relevantLtid)) {
				Set<String> isbns = ltidToIsbn.get(relevantLtid);
				if (isbns == null) {
					LOGGER.log(Level.SEVERE,
							"puuu, couldn't find isbns for ltid: "
									+ relevantLtid);
					continue;
				}
				for (String isbn : isbns) {
					sb.append(relevantLtid + ": (" + isbn + ", "
							+ aiwm.getWeight(isbn) + ") ");
				}
				sb.append("\n");
			}
			if (counter++ > 10)
				break;
		}
		sb.append("-------------------------------------\n");
		return sb.toString();
	}

	private List<QueryResult> convertIsbnToLtidAndFilter(
			List<QueryResult> results) {
		// updateing qrels of queries
		Map<String, String> isbnToLtid = AmazonIsbnConverter
				.loadIsbnToLtidMap(AmazonDirectoryInfo.ISBN_DICT);
		for (QueryResult res : results) {
			List<String> oldResults = res.getTopResults();
			List<String> newResults = new ArrayList<String>();
			List<String> oldResultsTitle = res.getTopResultsTitle();
			List<String> newResultsTitle = new ArrayList<String>();
			for (int i = 0; i < oldResults.size(); i++) {
				String isbn = oldResults.get(i);
				String ltid = isbnToLtid.get(isbn);
				if (ltid == null) {
					LOGGER.log(Level.SEVERE, "Couldn't find ISBN: " + isbn
							+ " in dict");
					continue;
				}
				if (!newResults.contains(ltid)) {
					newResults.add(ltid);
					newResultsTitle.add(oldResultsTitle.get(i));
				}
			}
			res.setTopResults(newResults);
			res.setTopResultsTitle(newResultsTitle);
		}
		return results;
	}

}
