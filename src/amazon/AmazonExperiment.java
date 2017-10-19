package amazon;

import indexing.InexFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
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
				totalPartitionNo, "reviews", AmazonDirectoryInfo.HOME
						+ "data/path_counts/amazon_path_reviews.csv");
		experiment.buildGlobalIndex();
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
		Map<String, Set<String>> ltidToIsbns = AmazonIsbnConverter
				.loadLtidToIsbnMap(AmazonDirectoryInfo.ISBN_DICT);
		try (FileWriter fw = new FileWriter(resultDir.getAbsolutePath() + "/"
				+ expNo + ".csv");
				FileWriter fw2 = new FileWriter(AmazonDirectoryInfo.RESULT_DIR
						+ "amazon_" + expNo + ".log")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.resultString() + "\n");
				fw2.write(generateLog(mqr, ltidToIsbns) + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage());
		}
	}
	private String generateLog(QueryResult queryResult,
			Map<String, Set<String>> ltidToIsbn) {
		ExperimentQuery query = queryResult.query;
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + query.getId() + "\t" + queryResult.mrr() + "\n");
		sb.append("query: " + query.getText() + "\n\n");
		sb.append("|relevant tuples| = " + query.qrels.size() + "\n");
		sb.append("|returned results| = " + queryResult.topResults.size()
				+ "\n");
		int counter = 0;
		sb.append("returned results: \n");
		AmazonIsbnWeightMap aiwm = AmazonIsbnWeightMap
				.getInstance(isbnsFilePath);
		for (int i = 0; i < queryResult.topResults.size(); i++) {
			String returnedLtid = queryResult.topResults.get(i);
			String returnedTitle = queryResult.topResultsTitle.get(i);
			String isbn = returnedTitle
					.substring(0, returnedTitle.indexOf(':'));
			if (query.qrels.contains(returnedLtid)) {
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
		for (String relevantLtid : query.qrels) {
			if (!queryResult.topResults.contains(relevantLtid)) {
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
			List<String> oldResults = res.topResults;
			List<String> newResults = new ArrayList<String>();
			List<String> oldResultsTitle = res.topResultsTitle;
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
			res.topResults = newResults;
			res.topResultsTitle = newResultsTitle;
		}
		return results;
	}

	public static void generateTrainingData(int expNo, int total,
			String indexPath) {
		LOGGER.log(Level.INFO, "Generating training data..");
		LOGGER.log(Level.INFO, "Loading queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				AmazonDirectoryInfo.QUERY_FILE, AmazonDirectoryInfo.QREL_FILE,
				"mediated_query", "title", "group", "narrative");
		Map<String, String> qidToQueryText = new HashMap<String, String>();
		for (ExperimentQuery query : queries) {
			qidToQueryText.put(query.getId() + "", query.getText());
		}
		try (BufferedReader br = new BufferedReader(new FileReader("???"));
				IndexReader reader = DirectoryReader.open(FSDirectory
						.open(Paths.get(indexPath)));
				FileWriter fw = new FileWriter("???")) {
			LOGGER.log(Level.INFO,
					"Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			String line = br.readLine();
			while (line != null) {
				Pattern ptr = Pattern
						.compile("(\\d+)\\sQ?0\\s(\\w+)\\s([0-9])");
				Matcher m = ptr.matcher(line);
				if (m.find()) {
					StringBuilder sb = new StringBuilder();
					String qid = m.group(1);
					int rel = Integer.parseInt(m.group(2));
					String label = m.group(3);
					String text = qidToQueryText.get(qid);
					sb.append(qid + ", ");
					for (int i = 0; i < 5; i++) {
						Map<String, Float> fieldToBoost = new HashMap<String, Float>();
						fieldToBoost.put(AmazonDocumentField.TITLE.toString(),
								i == 0 ? 1f : 0f);
						fieldToBoost.put(AmazonDocumentField.CREATORS
								.toString(), i == 1 ? 1f : 0f);
						fieldToBoost.put(AmazonDocumentField.TAGS.toString(),
								i == 2 ? 1f : 0f);
						fieldToBoost.put(AmazonDocumentField.DEWEY.toString(),
								i == 3 ? 1f : 0f);
						fieldToBoost.put(
								AmazonDocumentField.CONTENT.toString(), i == 4
										? 1f
										: 0f);
						Query query = QueryServices.buildLuceneQuery(text,
								fieldToBoost);
						sb.append(searcher.explain(query, rel).getValue());
						// TODO rel to isbn
						sb.append(",");
					}
					sb.append(label);
					sb.append("\n");
					fw.write(sb.toString());
				} else {
					LOGGER.log(Level.WARNING, "regex failed for line: " + line);
				}
				line = br.readLine();
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "QREL file not found!");
		}
	}

}
