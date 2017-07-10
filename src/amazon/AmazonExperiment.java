package amazon;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
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
		String indexName = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "amazon_p" + total + "_bm5_freq/" + expNo;
		buildGlobalIndex(expNo, total, indexName);
		Map<String, Float> fieldBoostMap = gridSearchOnGlobalIndex(expNo, total, indexName);
		expOnGlobalIndex(expNo, total, indexName, fieldBoostMap, "bm5_freq.csv");
//		indexName = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "amazon_p" + total + "_bm4_freq/" + expNo;
//		fieldBoostMap = gridSearchOnGlobalIndex(expNo, total, indexName);
//		expOnGlobalIndex(expNo, total, indexName, fieldBoostMap, "new.csv");
	}

	public static List<InexFile> buildSortedPathRating(String datasetPath) {
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

	public static void buildGlobalIndex(int expNo, int total, String indexName) {
		List<InexFile> fileList = InexFile.loadInexFileList(AmazonDirectoryInfo.FILE_LIST);
		LOGGER.log(Level.INFO, "Building index..");
		fileList = fileList.subList(0, (fileList.size() * expNo) / total);
		float[] fieldBoost = { 1f, 1f, 1f, 1f, 1f };
		new AmazonIndexer().buildIndex(fileList, indexName, fieldBoost);
		LOGGER.log(Level.INFO,
				"============\n" + "Number of files missing dewey: " + AmazonIndexer.getMissingDeweyCounter());
	}

	public static Map<String, Float> gridSearchOnGlobalIndex(int expNo, int total, String indexName) {
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(AmazonDirectoryInfo.QUERY_FILE,
				AmazonDirectoryInfo.QREL_FILE, "mediated_query", "title", "group", "narrative");
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		float[] p20 = new float[5];
		for (int i = 0; i < 5; i++) {
			Map<String, Float> fieldToBoost = new HashMap<String, Float>();
			fieldToBoost.put(AmazonDocumentField.TITLE.toString(), i == 0 ? 1f : 0f);
			fieldToBoost.put(AmazonDocumentField.CREATOR.toString(), i == 1 ? 1f : 0f);
			fieldToBoost.put(AmazonDocumentField.TAGS.toString(), i == 2 ? 1f : 0f);
			fieldToBoost.put(AmazonDocumentField.DEWEY.toString(), i == 3 ? 1f : 0f);
			fieldToBoost.put(AmazonDocumentField.CONTENT.toString(), i == 4 ? 1f : 0f);
			LOGGER.log(Level.INFO, i + ": " + fieldToBoost.toString());
			List<QueryResult> results = QueryServices.runQueriesWithBoosting(queries, indexName, new BM25Similarity(),
					fieldToBoost);
			convertIsbnToLtidAndFilter(results);
			for (QueryResult queryResult : results) {
				p20[i] += queryResult.precisionAtK(20);
			}
			p20[i] /= results.size();
		}
		LOGGER.log(Level.INFO, "Results of field as a document retrieval: " + Arrays.toString(p20));
		// best params bm3 are 1, 0, 2
		// best params bm4 are 0.2 0.1 0.06 0.65
		// best params bm4, query4 0.18 0.03 0.03 0.76
		Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		for (int i = 0; i < 5; i++) {
			fieldToBoost.put(AmazonDocumentField.TITLE.toString(), p20[i]);
			fieldToBoost.put(AmazonDocumentField.CREATOR.toString(), p20[i]);
			fieldToBoost.put(AmazonDocumentField.TAGS.toString(), p20[i]);
			fieldToBoost.put(AmazonDocumentField.DEWEY.toString(), p20[i]);
			fieldToBoost.put(AmazonDocumentField.CONTENT.toString(), p20[i]);
		}
		return fieldToBoost;
	}

	public static void expOnGlobalIndex(int expNo, int total, String indexName, 
			Map<String, Float> fieldToBoost, String outputName) {
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(AmazonDirectoryInfo.QUERY_FILE,
				AmazonDirectoryInfo.QREL_FILE, "mediated_query", "title", "group", "narrative");
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		// Map<String, Float> fieldToBoost = new HashMap<String, Float>();
		// fieldToBoost.put(AmazonIndexer.TITLE_ATTRIB, 0.18f);
		// fieldToBoost.put(AmazonIndexer.CREATOR_ATTRIB, 0.03f);
		// fieldToBoost.put(AmazonIndexer.TAGS_ATTRIB, 0.03f);
		// fieldToBoost.put(AmazonIndexer.CONTENT_ATTRIB, 0.76f);
		List<QueryResult> results = QueryServices.runQueriesWithBoosting(queries, indexName, new BM25Similarity(),
				fieldToBoost);
		LOGGER.log(Level.INFO, "updating ISBN results to LTID..");
		convertIsbnToLtidAndFilter(results);
		LOGGER.log(Level.INFO, "Preparing ltid -> InexFile map..");
		// preparing ltid -> inex file map
		List<InexFile> inexFiles = InexFile.loadInexFileList(AmazonDirectoryInfo.FILE_LIST);
		Map<String, InexFile> ltidToInexFile = new HashMap<String, InexFile>();
		int missedIsbnCount = 0;
		for (InexFile inexFile : inexFiles) {
			String isbn = FilenameUtils.removeExtension(new File(inexFile.path).getName());
			String ltid = isbnToLtid.get(isbn);
			if (ltid != null) {
				ltidToInexFile.put(ltid, inexFile);
			} else {
				LOGGER.log(Level.SEVERE, "isbn: " + isbn + "(extracted from filename) does not exists in dict");
				missedIsbnCount++;
			}
		}
		LOGGER.log(Level.INFO, "Number of missed ISBNs extracted from filename in dict: " + missedIsbnCount);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(AmazonDirectoryInfo.RESULT_DIR + outputName)) {
			// FileWriter fw2 = new FileWriter(AmazonDirectoryInfo.RESULT_DIR +
			// "amazon_" + expNo + ".log")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.resultString() + "\n");
				// fw2.write(generateLog(mqr, ltidToInexFile) + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage());
		}
	}

	public static void generateTrainingData(int expNo, int total, String indexPath) {
		LOGGER.log(Level.INFO, "Generating training data..");
		LOGGER.log(Level.INFO, "Loading queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(AmazonDirectoryInfo.QUERY_FILE,
				AmazonDirectoryInfo.QREL_FILE, "mediated_query", "title", "group", "narrative");
		Map<String, String> qidToQueryText = new HashMap<String, String>();
		for (ExperimentQuery query : queries) {
			qidToQueryText.put(query.getId() + "", query.getText());
		}
		try (BufferedReader br = new BufferedReader(new FileReader("???"));
				IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
				FileWriter fw = new FileWriter("???")) {
			LOGGER.log(Level.INFO, "Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			String line = br.readLine();
			while (line != null) {
				Pattern ptr = Pattern.compile("(\\d+)\\sQ?0\\s(\\w+)\\s([0-9])");
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
						fieldToBoost.put(AmazonDocumentField.TITLE.toString(), i == 0 ? 1f : 0f);
						fieldToBoost.put(AmazonDocumentField.CREATOR.toString(), i == 1 ? 1f : 0f);
						fieldToBoost.put(AmazonDocumentField.TAGS.toString(), i == 2 ? 1f : 0f);
						fieldToBoost.put(AmazonDocumentField.DEWEY.toString(), i == 3 ? 1f : 0f);
						fieldToBoost.put(AmazonDocumentField.CONTENT.toString(), i == 4 ? 1f : 0f);
						Query query = QueryServices.buildLuceneQuery(text, fieldToBoost);
						sb.append(searcher.explain(query, rel).getValue()); // TODO
																			// rel
																			// is
																			// isbn?
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

	static String generateLog(QueryResult queryResult, Map<String, InexFile> ltidToInexfile) {
		ExperimentQuery query = queryResult.query;
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + query.getId() + "\t" + query.getText() + "\n");
		sb.append("|relevant tuples| = " + query.qrels.size() + "\n");
		sb.append("|returned results| = " + queryResult.topResults.size() + "\n");
		int counter = 0;
		sb.append("returned results: \n");
		for (int i = 0; i < queryResult.topResults.size(); i++) {
			String returnedLtid = queryResult.topResults.get(i);
			String returnedTitle = queryResult.topResultsTitle.get(i);
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
				if (inFile == null) {
					LOGGER.log(Level.SEVERE, "No Inex File for ltid: " + relevantLtid);
					sb.append("-- " + relevantLtid + " (no inex file) " + "\n");
				} else {
					sb.append("-- " + relevantLtid + "\t" + inFile.path + "\t\t" + inFile.weight + "\n");
				}
			}
			if (counter++ > 20)
				break;
		}
		sb.append("-------------------------------------\n");
		return sb.toString();
	}

	private static Map<String, String> loadIsbnToLtidMap() {
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

	private static List<QueryResult> convertIsbnToLtidAndFilter(List<QueryResult> results) {
		// updateing qrels of queries
		for (QueryResult res : results) {
			List<String> oldResults = res.topResults;
			List<String> newResults = new ArrayList<String>();
			List<String> oldResultsTitle = res.topResultsTitle;
			List<String> newResultsTitle = new ArrayList<String>();
			for (int i = 0; i < oldResults.size(); i++) {
				String isbn = oldResults.get(i);
				if (!isbnToLtid.containsKey(isbn)) {
					LOGGER.log(Level.SEVERE, "Couldn't find ISBN: " + isbn + " in dict");
					continue;
				}
				String ltid = isbnToLtid.get(isbn);
				if (!newResults.contains(ltid))
					newResults.add(ltid);
				newResultsTitle.add(oldResultsTitle.get(i));
			}
			res.topResults = newResults;
			res.topResultsTitle = newResultsTitle;
		}
		return results;
	}

}
