package amazon;

import indexing.InexDatasetIndexer;
import indexing.InexFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import amazon.datatools.AmazonDeweyConverter;
import amazon.datatools.AmazonIsbnConverter;
import amazon.indexing.AmazonFileIndexer;
import amazon.popularity.AmazonIsbnPopularityMap;
import amazon.query.AmazonQueryResultProcessor;

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
	private String experimentName;
	private String indexPath;
	private String isbnsFilePath;

	public AmazonExperiment(int experimentNumber, int partitionCount,
			String experimentName, String isbnsFilePath) {
		this.expNo = experimentNumber;
		this.total = partitionCount;
		this.experimentName = experimentName;
		indexPath = AmazonDirectoryInfo.GLOBAL_INDEX_DIR + experimentName + "/"
				+ expNo;
		this.isbnsFilePath = isbnsFilePath;
	}

	public static void main(String[] args) {
		if (args.length < 3) {
			LOGGER.log(Level.SEVERE,
					"Input argument is missing. Terminating the program..");
			return;
		}
		int expNo = Integer.parseInt(args[0]);
		int totalPartitionNo = Integer.parseInt(args[1]);
		String experimentName = args[2];
		String amazonPathFile = args[3];
		AmazonExperiment experiment = new AmazonExperiment(expNo,
				totalPartitionNo, experimentName, amazonPathFile);
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
				AmazonDirectoryInfo.QUERY_FILE, AmazonDirectoryInfo.QREL_FILE,
				experiment.queryFields, true);
	}

	void buildGlobalIndex() {
		List<InexFile> fileList = InexFile.loadInexFileList(this.isbnsFilePath);
		LOGGER.log(Level.INFO, "Building index..");
		fileList = fileList.subList(0, (fileList.size() * expNo) / total);
		AmazonFileIndexer fileIndexer = new AmazonFileIndexer(fields,
				AmazonIsbnConverter
						.loadIsbnToLtidMap(AmazonDirectoryInfo.ISBN_DICT),
				AmazonDeweyConverter
						.getInstance(AmazonDirectoryInfo.DEWEY_DICT));
		InexDatasetIndexer datasetIndexer = new InexDatasetIndexer(
				fileIndexer);
		datasetIndexer.buildIndex(fileList, indexPath);
	}

	Map<String, Float> gridSearchOnGlobalIndex(String queryFile,
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

	void expOnGlobalIndex(Map<String, Float> fieldToBoost, String queryFile,
			String qrelFile, String[] queryFields, boolean extraLogging) {
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				queryFile, qrelFile, queryFields);
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(
				queries, indexPath, new BM25Similarity(), fieldToBoost);
		LOGGER.log(Level.INFO, "updating ISBN results to LTID..");
		Map<String, String> isbnToLtid = AmazonIsbnConverter
				.loadIsbnToLtidMap(AmazonDirectoryInfo.ISBN_DICT);
		for (QueryResult qr : results) {
			AmazonQueryResultProcessor.convertIsbnAnswersToLtidAndFilter(qr,
					isbnToLtid);
		}
		LOGGER.log(Level.INFO, "Writing results to file..");
		File resultDir = new File(AmazonDirectoryInfo.RESULT_DIR + "amazon_f"
				+ fields.length + "_" + this.experimentName);
		resultDir.mkdirs();
		String resultPath = resultDir.getAbsolutePath() + "/" + expNo + ".csv";
		if (extraLogging) {
			Map<String, Set<String>> ltidToIsbns = AmazonIsbnConverter
					.loadLtidToIsbnMap(AmazonDirectoryInfo.ISBN_DICT);
			AmazonIsbnPopularityMap aipm = AmazonIsbnPopularityMap
					.getInstance(isbnsFilePath);
			String logPath = AmazonDirectoryInfo.RESULT_DIR
					+ this.experimentName + "_" + expNo + ".log";
			try (FileWriter fw = new FileWriter(resultPath);
					FileWriter fw2 = new FileWriter(logPath);
					IndexReader reader = DirectoryReader.open(FSDirectory
							.open(Paths.get(indexPath)))) {
				for (QueryResult mqr : results) {
					fw.write(mqr.resultString() + "\n");
					fw2.write(AmazonQueryResultProcessor.generateLog(mqr,
							ltidToIsbns, aipm, reader) + "\n");
				}
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage());
			}
		} else {
			try (FileWriter fw = new FileWriter(resultPath)) {
				for (QueryResult mqr : results) {
					fw.write(mqr.resultString() + "\n");
				}
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage());
			}
		}
	}
}
