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

public class AmazonMapleExperiment {

	private static final Logger LOGGER = Logger
			.getLogger(AmazonMapleExperiment.class.getName());
	private static final String DATA_FOLDER = "/data/ghadakcv/data/";
	private static final String FILE_LIST = DATA_FOLDER
			+ "amazon_path_ratecount.csv";
	private static final String ISBN_DICT_PATH = DATA_FOLDER
			+ "amazon-lt.isbn.thingID";
	private static final String DEWEY_DICT = DATA_FOLDER + "dewey.tsv";
	private static final String INDEX_PATH = DATA_FOLDER + "index";
	private static final String RESULT_DIR = DATA_FOLDER + "result/";
	private static final String QUERY_FILE = DATA_FOLDER + "inex14sbs.topics.xml";
	private static final String QREL_FILE = DATA_FOLDER + "inex14sbs.qrels";
	private static final String LTID_LIST = DATA_FOLDER + "ltid_ratecountsum.sor";

	private static final AmazonDocumentField[] fields = {
			AmazonDocumentField.TITLE, AmazonDocumentField.CONTENT,
			AmazonDocumentField.CREATORS, AmazonDocumentField.TAGS,
			AmazonDocumentField.DEWEY};
	private static final String[] queryFields = {"mediated_query", "title",
			"group", "narrative"};

	
	static void buildIndex(){
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
		List<String> sortedLtidList = new ArrayList<String>();
		for (String line : Files.readAllLines(Paths.get(LTID_LIST))){
			String[] fields = line.split(" ");
			if (fields.length > 1){
				sortedLtidList.add(fields[0]);
			} else {
				LOGGER.log(Level.SEVERE, "ltid -> weight line doens't have enough fields");
			}
		}
		for (int i = 0; i < 50; i++) {
			List<QueryResult> results = QueryServices.runQueriesWithBoosting(
					queries, INDEX_PATH, new BM25Similarity(), fieldBoostMap);

			LOGGER.log(Level.INFO, "updating ISBN results to LTID..");
			int subsetSize = (int) (i * sortedLtidList.size() / 50.0);
			convertIsbnToLtidAndFilter(results, new TreeSet<String>(
					sortedLtidList.subList(0, subsetSize)));
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
	private static List<QueryResult> convertIsbnToLtidAndFilter(
			List<QueryResult> results, TreeSet<String> cache) {
		// updateing qrels of queries
		Map<String, String> isbnToLtid = AmazonIsbnConverter
				.loadIsbnToLtidMap(ISBN_DICT_PATH);
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
				if (!newResults.contains(ltid) && cache.contains(ltid)) {
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
