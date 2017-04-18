package imdb;

import indexing.InexFileMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki_inex09.Utils;
import wiki_inex13.Wiki13Indexer;

public class ImdbExperiment {

	static final Logger LOGGER = Logger.getLogger(ImdbExperiment.class
			.getName());

	public static void main(String[] args) {
		long start_t = System.currentTimeMillis();
		gridSearchExperiment(0.15f);
		System.out.println((System.currentTimeMillis() - start_t)/1000);
	}
	
	private static List<InexFileMetadata> buildSortedPathRating(String datasetPath){
		List<InexFileMetadata> pathCount = new ArrayList<InexFileMetadata>();
		List<String> filePaths = Utils
				.listFilesForFolder(new File(datasetPath));
		for (String filepath : filePaths) {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			try {
				DocumentBuilder db = dbf.newDocumentBuilder();
				org.w3c.dom.Document doc = db.parse(new File(filepath));
				NodeList nodeList = doc.getElementsByTagName("rating");
				if (nodeList.getLength() > 1) {
					LOGGER.log(Level.SEVERE, filepath
							+ " has more than one rating entries!");
				} else if (nodeList.getLength() < 1) {
					pathCount.add(new InexFileMetadata(filepath, 0.0));
				} else {
					Node node = nodeList.item(0).getFirstChild();
					if (node.getNodeValue() != null){
						String rating = node.getNodeValue().split(" ")[0];
						pathCount.add(new InexFileMetadata(filepath, Double.parseDouble(rating)));
					} else {
						pathCount.add(new InexFileMetadata(filepath, 0.0));
					}
				}
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(pathCount);
		try (FileWriter fw = new FileWriter("data/path_ratings.csv")){
			for (InexFileMetadata dfm : pathCount){
				fw.write(dfm.path + "," + dfm.weight + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (pathCount);
	}
	
	private List<InexFileMetadata> loadPathRating(String filepath){
		List<InexFileMetadata> list = new ArrayList<InexFileMetadata>();
		try (BufferedReader br = new BufferedReader(new FileReader(filepath))){
			String line = br.readLine();
			while(line != null){
				String path = line.split(" ")[0];
				double rating = Double.parseDouble(line.split(" ")[1]);
				list.add(new InexFileMetadata(path, rating));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static void gridSearchExperiment(float gamma) {
		// Note that the path count should be sorted!
		List<InexFileMetadata> pathCountList = buildSortedPathRating("/scratch/data-sets/beautified-imdb-2010-4-10/movies/");
		pathCountList = pathCountList.subList(0, pathCountList.size() / 10);
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + pathCountList.size());
		String indexName = "data/index/imdb";
		LOGGER.log(Level.INFO, "Building index..");
		Wiki13Indexer.buildTextIndex(pathCountList, indexName, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				"data/queries/imdb/2010-topics.xml",
				"data/queries/imdb/inex2010-dc-article.qrels");
		queries = queries.subList(0, queries.size());
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<QueryResult> results = QueryServices
				.runQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter("data/result/imdb/" + "result.csv");
				FileWriter fw2 = new FileWriter("data/result/imdb/" + "top.csv")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.toString() + "\n");
				fw2.write(mqr.top10() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
//		try {
//			LOGGER.log(Level.INFO, "cleanup..");
//			FileUtils.deleteDirectory(new File(indexName));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
}
