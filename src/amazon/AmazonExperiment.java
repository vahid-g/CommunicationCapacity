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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.lucene.search.similarities.BM25Similarity;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import imdb.ImdbIndexer;
import indexing.InexFile;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki_inex09.ClusterDirectoryInfo;
import wiki_inex09.Utils;

public class AmazonExperiment {

	static final Logger LOGGER = Logger.getLogger(AmazonExperiment.class.getName());

	public static void main(String[] args) {
		// buildSortedPathRating(AmazonDirectoryInfo.DATA_SET);
		gridSearchExperiment();
	}

	static List<InexFile> buildSortedPathRating(String datasetPath) {
		List<InexFile> pathCount = new ArrayList<InexFile>();
		List<String> filePaths = Utils.listFilesForFolder(new File(datasetPath));
		for (String filepath : filePaths) {
			if (filepath.contains(".dtd")) continue;
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
	
	public static void gridSearchExperiment() {
		// Note that the path count should be sorted!
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle(AmazonDirectoryInfo.FILE_LIST);
		// fileList = fileList.subList(0, fileList.size() / 100);
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + fileList.size());
		String indexName = AmazonDirectoryInfo.LOCAL_INDEX + "amazon_p1_bm_sample";
		LOGGER.log(Level.INFO, "Building index..");
		float[] fieldBoost = {1f, 1f, 1f, 1f, 1f};
		new AmazonIndexer().buildIndex(fileList, indexName, new BM25Similarity(), fieldBoost);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				AmazonDirectoryInfo.QUERY_FILE,
				AmazonDirectoryInfo.QREL_FILE);
		mapIsbnToLT(queries, AmazonDirectoryInfo.ISBN_DICT);
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		List<List<QueryResult>> allResults = new ArrayList<List<QueryResult>>();
		for (int i = 0; i < 64; i++) {
			Map<String, Float> fieldToBoost = new HashMap<String, Float>();
			fieldToBoost.put(AmazonIndexer.TITLE_ATTRIB, i % 4 + 1.0f);
			fieldToBoost.put(AmazonIndexer.CREATOR_ATTRIB, (i / 4) % 4 + 1.0f);
			fieldToBoost.put(AmazonIndexer.CONTENT_ATTRIB, (i / 16) % 4 + 1.0f);
			LOGGER.log(Level.INFO, i + ": " + fieldToBoost.toString());
			List<QueryResult> results = QueryServices.runQueriesWithBoosting(
					queries, indexName, new BM25Similarity(), fieldToBoost);
			allResults.add(results);
			break;
		}
		
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(AmazonDirectoryInfo.RESULT_DIR + 
				"param_compare.csv");
				FileWriter fw2 = new FileWriter(AmazonDirectoryInfo.RESULT_DIR + 
						"param_compare.log")) {
			for (int i = 0; i < queries.size(); i++) {
				fw.write(allResults.get(0).get(i).query.text + ",");
				for (int j = 0; j < allResults.size(); j++) {
					fw.write(allResults.get(j).get(i).precisionAtK(20) + ",");
					fw.write(allResults.get(j).get(i).top10());
				}
				fw.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		// best params are 
	}

	public static void buildGlobalIndex(int expNo, int total) {
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle(AmazonDirectoryInfo.FILE_LIST);
		LOGGER.log(Level.INFO, "Building index..");
		String indexName = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "amazon_"
				+ total + "_" + expNo;
		fileList = fileList.subList(0, (fileList.size() * expNo) / total);
		float[] fieldBoost = {1f, 1f, 1f, 1f, 1f};
		new ImdbIndexer().buildIndex(fileList, indexName, fieldBoost);
	}
	
	private static void mapIsbnToLT(List<ExperimentQuery> queries, String dictPath){
		try (BufferedReader br = new BufferedReader(new FileReader(dictPath))) {
			// loading dictionary from the file
			Map<String, String> idToIsbn = new HashMap<String, String>();
			String line = br.readLine();
			while (line != null) {
				String[] ids = line.split(",");
				idToIsbn.put(ids[1], ids[0]);
				line = br.readLine();
			}
			LOGGER.log(Level.INFO, "ISBN dict size: " + idToIsbn.size());
			
			// updateing qrels of queries
			for (ExperimentQuery query : queries){
				Set<String> oldQrels = query.qrels;
				Set<String> newQrels = new HashSet<String>();
				for (String qrel : oldQrels){
					if (!idToIsbn.containsKey(qrel)) {
						LOGGER.log(Level.SEVERE, "Couldn't find ISBN for LT ID: " + qrel);
						continue;
					}
					newQrels.add(idToIsbn.get(qrel));
				}
				query.qrels = newQrels;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
