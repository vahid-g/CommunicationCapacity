package amazon;

import indexing.InexFile;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.search.similarities.BM25Similarity;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import amazon.datatools.AmazonDeweyConverter;
import amazon.datatools.AmazonIsbnConverter;
import amazon.indexing.AmazonDatasetIndexer;
import amazon.indexing.AmazonIndexer;
import amazon.query.AmazonQueryResultProcessor;

public class AmazonMapleExperiment {

	private static final Logger LOGGER = Logger
			.getLogger(AmazonMapleExperiment.class.getName());
	private static final String DATA_FOLDER = "/data/ghadakcv/data/";
	private static final String FILE_LIST = DATA_FOLDER
			+ "amazon_path_integ.csv";
	private static final String ISBN_DICT_PATH = DATA_FOLDER
			+ "amazon-lt.isbn.thingID";
	private static final String DEWEY_DICT = DATA_FOLDER + "dewey.tsv";
	private static final String INDEX_PATH = DATA_FOLDER + "index";
	private static final String RESULT_DIR = DATA_FOLDER + "result/";
	private static final String QUERY_FILE = DATA_FOLDER
			+ "inex14sbs.topics.xml";
	private static final String QREL_FILE = DATA_FOLDER + "inex14sbs.qrels";
	private static final AmazonDocumentField[] fields = {
			AmazonDocumentField.TITLE, AmazonDocumentField.CONTENT,
			AmazonDocumentField.CREATORS, AmazonDocumentField.TAGS,
			AmazonDocumentField.DEWEY};
	private static final String[] queryFields = {"mediated_query", "title",
			"group", "narrative"};

	static void buildIndex() {
		List<InexFile> fileList = InexFile.loadInexFileList(FILE_LIST);
		LOGGER.log(Level.INFO, "Building index..");
		AmazonIndexer fileIndexer = new AmazonIndexer(fields,
				AmazonIsbnConverter.loadIsbnToLtidMap(ISBN_DICT_PATH),
				AmazonDeweyConverter.getInstance(DEWEY_DICT));
		AmazonDatasetIndexer datasetIndexer = new AmazonDatasetIndexer(
				fileIndexer);
		datasetIndexer.buildIndex(fileList, INDEX_PATH);
	}

	public static void main(String[] args) throws IOException {
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				QUERY_FILE, QREL_FILE, queryFields);
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		Map<String, Float> fieldBoostMap = new HashMap<String, Float>();
		fieldBoostMap.put(AmazonDocumentField.TITLE.toString(), 0.22f);
		fieldBoostMap.put(AmazonDocumentField.CONTENT.toString(), 0.62f);
		fieldBoostMap.put(AmazonDocumentField.CREATORS.toString(), 0.04f);
		fieldBoostMap.put(AmazonDocumentField.TAGS.toString(), 0.1f);
		fieldBoostMap.put(AmazonDocumentField.DEWEY.toString(), 0.02f);
		// List<String> sortedLtidList = loadLtidList(DATA_FOLDER
		// + "ltid_ratecountsum.sor");
		List<String> sortedLtidList = loadIsbnList(FILE_LIST);
		Map<String, String> isbnToLtid = AmazonIsbnConverter
				.loadIsbnToLtidMap(ISBN_DICT_PATH);
		for (int i = 1; i <= 50; i++) {
			List<QueryResult> results = QueryServices.runQueriesWithBoosting(
					queries, INDEX_PATH, new BM25Similarity(), fieldBoostMap);
			int subsetSize = (int) (i * sortedLtidList.size() / 50.0);
			TreeSet<String> cache = new TreeSet<String>(sortedLtidList.subList(
					0, subsetSize));
			for (QueryResult result : results) {
				filterCacheResults(result, cache);
				AmazonQueryResultProcessor.convertIsbnAnswersToLtidAndFilter(
						result, isbnToLtid);
				// filterLtidCacheResults(result, cache);
			}

			LOGGER.log(Level.INFO, "Writing results to file..");
			try (FileWriter fw = new FileWriter(RESULT_DIR + i + ".csv")) {
				for (QueryResult mqr : results) {
					fw.write(mqr.resultString() + "\n");
				}
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage());
			}
		}
	}

	// format of the input file is <ltid w>
	static List<String> loadLtidList(String ltidListPath) throws IOException {
		List<String> sortedLtidList = new ArrayList<String>();
		for (String line : Files.readAllLines(Paths.get(ltidListPath))) {
			String[] fields = line.split(" ");
			if (fields.length > 1) {
				sortedLtidList.add(fields[0]);
			} else {
				LOGGER.log(Level.SEVERE,
						"ltid -> weight line doens't have enough fields");
			}
		}
		return sortedLtidList;
	}

	// format of the input file is <path/isbn.xml,w>
	static List<String> loadIsbnList(String isbnListPath) throws IOException {
		List<String> sortedIsbnList = new ArrayList<String>();
		for (String line : Files.readAllLines(Paths.get(isbnListPath))) {
			if (line.contains("/") && line.contains(".")) {
				String isbn = line.substring(line.lastIndexOf('/') + 1,
						line.indexOf('.'));
				sortedIsbnList.add(isbn);
			} else {
				LOGGER.log(Level.SEVERE, "couldn't parse: " + line);
			}
		}
		return sortedIsbnList;
	}
	static void filterCacheResults(QueryResult queryResult,
			TreeSet<String> cache) {
		List<String> topResults = queryResult.getTopResults();
		List<String> topResultTitle = queryResult.getTopResultsTitle();
		for (int i = 0; i < topResults.size(); i++) {
			if (!cache.contains(topResults.get(i))) {
				topResults.remove(topResults.get(i));
				topResultTitle.remove(topResultTitle.get(i));
			}
		}
	}

}
