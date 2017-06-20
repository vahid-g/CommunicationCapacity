package amazon;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import indexing.InexFile;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki_inex09.ClusterDirectoryInfo;
import wiki_inex09.Utils;

public class AmazonExperiment {

	static final Logger LOGGER = Logger.getLogger(AmazonExperiment.class.getName());

	static Map<String, String> isbnToLtid = loadIsbnToLtidMap();

	public static void main(String[] args) {
		int expNo = Integer.parseInt(args[0]);
		int total = Integer.parseInt(args[1]);
		// buildSortedPathRating(AmazonDirectoryInfo.DATA_SET);
		// gridSearchExperiment(expNo, total);
		// gridSearchOnGlobalIndex();
		// buildGlobalIndex(expNo, total);
		 expOnGlobalIndex(expNo, total);
	}

	static List<InexFile> buildSortedPathRating(String datasetPath) {
		List<InexFile> pathCount = new ArrayList<InexFile>();
		List<String> filePaths = Utils.listFilesForFolder(new File(datasetPath));
		for (String filepath : filePaths) {
			if (filepath.contains(".dtd"))
				continue;
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			try {
				DocumentBuilder db = dbf.newDocumentBuilder();
				org.w3c.dom.Document doc = db.parse(new File(filepath));
				NodeList nodeList = doc.getElementsByTagName("review");
				pathCount.add(new InexFile(filepath, nodeList.getLength()));
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(pathCount);
		try (FileWriter fw = new FileWriter(AmazonDirectoryInfo.FILE_LIST)) {
			for (InexFile dfm : pathCount) {
				fw.write(dfm.path + "," + dfm.weight + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (pathCount);
	}

	public static void gridSearchExperiment(int expNo, int total) {
		// Note that the path count should be sorted!
		List<InexFile> fileList = InexFile.loadInexFileList(AmazonDirectoryInfo.FILE_LIST);
		fileList = fileList.subList(0, (fileList.size() * expNo) / total);
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + fileList.size());
		String indexName = AmazonDirectoryInfo.LOCAL_INDEX + "amazon_p" + total + "_def/" + expNo;
		LOGGER.log(Level.INFO, "Building index..");
		float[] fieldBoost = { 1f, 1f, 1f, 1f, 1f };
		new AmazonIndexer().buildIndex(fileList, indexName, new BM25Similarity(), fieldBoost);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(AmazonDirectoryInfo.QUERY_FILE,
				AmazonDirectoryInfo.QREL_FILE, "mediated_query", "title");
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		List<List<QueryResult>> allResults = new ArrayList<List<QueryResult>>();
		for (int i = 0; i < 4; i++) {
			Map<String, Float> fieldToBoost = new HashMap<String, Float>();
			fieldToBoost.put(AmazonIndexer.TITLE_ATTRIB, i == 0 ? 1f : 0f);
			fieldToBoost.put(AmazonIndexer.CREATOR_ATTRIB, i == 1 ? 1f : 0f);
			fieldToBoost.put(AmazonIndexer.TAGS_ATTRIB, i == 2 ? 1f : 0f);
			fieldToBoost.put(AmazonIndexer.CONTENT_ATTRIB, i == 3 ? 1f : 0f);
			LOGGER.log(Level.INFO, i + ": " + fieldToBoost.toString());
			List<QueryResult> results = QueryServices.runQueriesWithBoosting(queries, indexName, new BM25Similarity(),
					fieldToBoost);
			allResults.add(convertIsbnToLtid(results));
		}
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(AmazonDirectoryInfo.RESULT_DIR + "grid_bm_f4.csv")) {
			for (int i = 0; i < queries.size(); i++) {
				fw.write(allResults.get(0).get(i).query.getText() + ",");
				for (int j = 0; j < allResults.size(); j++) {
					fw.write(allResults.get(j).get(i).precisionAtK(20) + ",");
				}
				fw.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		// best params bm3 are 1, 0, 2
		// best params bm4 are 0.2 0.1 0.06 0.65
		try {
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void gridSearchOnGlobalIndex() {
		int total = 50;
		int expNo = 50;
		String indexName = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "amazon_p" + total + "_bm/" + expNo;
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(AmazonDirectoryInfo.QUERY_FILE,
				AmazonDirectoryInfo.QREL_FILE, "mediated_query");
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		List<List<QueryResult>> allResults = new ArrayList<List<QueryResult>>();
		for (int i = 0; i < 3; i++) {
			Map<String, Float> fieldToBoost = new HashMap<String, Float>();
			fieldToBoost.put(AmazonIndexer.TITLE_ATTRIB, i == 0 ? 1f : 0f);
			fieldToBoost.put(AmazonIndexer.CREATOR_ATTRIB, i == 1 ? 1f : 0f);
			fieldToBoost.put(AmazonIndexer.CONTENT_ATTRIB, i == 2 ? 1f : 0f);
			LOGGER.log(Level.INFO, i + ": " + fieldToBoost.toString());
			List<QueryResult> results = QueryServices.runQueriesWithBoosting(queries, indexName, new BM25Similarity(),
					fieldToBoost);
			convertIsbnToLtid(results);
			allResults.add(results);
		}
		// best params are 0.017, 0.008, 0.043
		// normalized to 0.25, 0.11, 0.63
		// 0.27, 0.1, 0.63
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(AmazonDirectoryInfo.RESULT_DIR + "param_compare.csv")) {
			for (int i = 0; i < queries.size(); i++) {
				fw.write(allResults.get(0).get(i).query.getText() + ",");
				for (int j = 0; j < allResults.size(); j++) {
					fw.write(allResults.get(j).get(i).precisionAtK(20) + ",");
				}
				fw.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static Map<String, String> loadIsbnToLtidMap() {
		Map<String, String> isbnToLtid = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(AmazonDirectoryInfo.ISBN_DICT))) {
			String line = br.readLine();
			while (line != null) {
				String[] ids = line.split(",");
				isbnToLtid.put(ids[0], ids[1]);
				line = br.readLine();
			}
			LOGGER.log(Level.INFO, "ISBN dict size: " + isbnToLtid.size());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return isbnToLtid;
	}

	private static List<QueryResult> convertIsbnToLtid(List<QueryResult> results) {
		// updateing qrels of queries
		for (QueryResult res : results) {
			List<String> oldResults = res.topResults;
			List<String> newTopResults = new ArrayList<String>();
			for (String isbn : oldResults) {
				if (!isbnToLtid.containsKey(isbn)) {
					LOGGER.log(Level.SEVERE, "Couldn't find ISBN: " + isbn + " in dict");
					continue;
				}
				String ltid = isbnToLtid.get(isbn);
				if (!newTopResults.contains(ltid))
					newTopResults.add(ltid);
			}
			res.topResults = newTopResults;
		}
		return results;
	}

	public static void buildGlobalIndex(int expNo, int total) {
		List<InexFile> fileList = InexFile.loadInexFileList(AmazonDirectoryInfo.FILE_LIST);
		LOGGER.log(Level.INFO, "Building index..");
		String indexName = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "amazon_p" + total + "_bm4/" + expNo;
		fileList = fileList.subList(0, (fileList.size() * expNo) / total);
		float[] fieldBoost = { 1f, 1f, 1f, 1f, 1f };
		new AmazonIndexer().buildIndex(fileList, indexName, fieldBoost);
	}

	public static void expOnGlobalIndex(int expNo, int total) {
		String indexName = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "amazon_p" + total + "_bm4/" + expNo;
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(AmazonDirectoryInfo.QUERY_FILE,
				AmazonDirectoryInfo.QREL_FILE, "mediated_query", "title");
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		fieldToBoost.put(AmazonIndexer.TITLE_ATTRIB, 0.2f);
		fieldToBoost.put(AmazonIndexer.CREATOR_ATTRIB, 0.1f);
		fieldToBoost.put(AmazonIndexer.TAGS_ATTRIB, 0.06f);
		fieldToBoost.put(AmazonIndexer.CONTENT_ATTRIB, 0.64f);
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(queries, indexName, new BM25Similarity(),
				fieldToBoost);
		LOGGER.log(Level.INFO, "Converting ISBN results to LTID..");
		convertIsbnToLtid(results);
		LOGGER.log(Level.INFO, "Preparing ltid -> InexFile map..");
		// preparing ltid -> inex file map
		List<InexFile> inexFiles = InexFile.loadInexFileList(AmazonDirectoryInfo.FILE_LIST);
		Map<String, InexFile> ltidToInexFile = new HashMap<String, InexFile>();
		for (InexFile inexFile : inexFiles) {
			String isbn = FilenameUtils.removeExtension(new File(inexFile.path).getName());
			String ltid = isbnToLtid.get(isbn);
			if (ltid != null)
				ltidToInexFile.put(ltid, inexFile);
			else
				LOGGER.log(Level.SEVERE, "isbn: " + isbn + " does not exists in dict");
		}
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(AmazonDirectoryInfo.RESULT_DIR + "amazon_" + expNo + ".csv");
				FileWriter fw2 = new FileWriter(AmazonDirectoryInfo.RESULT_DIR + "amazon_" + expNo + ".log")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.resultString() + "\n");
				fw2.write(generateLog(mqr, ltidToInexFile) + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage());
		}
	}

	public static String generateLog(QueryResult queryResult, Map<String, InexFile> ltidToInexfile) {
		ExperimentQuery query = queryResult.query;
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + query.getId() + "\t" + query.getText() + "\n");
		sb.append("|relevant tuples| = " + query.qrels.size() + "\n");
		sb.append("|returned results| = " + queryResult.topResults.size() + "\n");
		int counter = 0;
		sb.append("returned results: \n");
		for (int i = 0; i < queryResult.topResults.size(); i++) {
			String returnedLtid = queryResult.topResults.get(i);
			String returnedTitle = queryResult.topResults.get(i);
			if (query.qrels.contains(returnedLtid)) {
				sb.append("++ " + returnedLtid + "\t" + returnedTitle + "\n");
			} else {
				sb.append("-- " + returnedLtid + "\t" + returnedTitle + "\n");
			}
			if (counter++ > 20)
				break;
		}
		counter = 0;
		sb.append("missed docs: \n");
		for (String relevantLtid : query.qrels) {
			if (!queryResult.topResults.contains(relevantLtid)) {
				InexFile inFile = ltidToInexfile.get(relevantLtid);
				if (inFile == null){
					LOGGER.log(Level.SEVERE, "No Inex File for ltid: " + relevantLtid);
				} else {
					sb.append("-- " + relevantLtid + "\t" + ltidToInexfile.get(relevantLtid).title + "\n");
				}
			}
			if (counter++ > 0)
				break;
		}
		sb.append("-------------------------------------\n");
		return sb.toString();
	}

}
