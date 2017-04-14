package imdb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import wiki_inex09.Utils;

public class ImdbExperiment {

	static final Logger LOGGER = Logger.getLogger(ImdbExperiment.class
			.getName());

	public static void main(String[] args) {
		long start_t = System.currentTimeMillis();
		Map<String, Double> map = buildMoviePathratingMap("");
		System.out.println((System.currentTimeMillis() - start_t)/1000);
	}
	
	public static Map<String, Double> buildMoviePathratingMap(String datasetPath){
		datasetPath = "/scratch/data-sets/beautified-imdb-2010-4-10/movies/";
		Map<String, Double> pathRatingMap = new HashMap<String, Double>();
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
					pathRatingMap.put(filepath, 0.0);
				} else {
					Node node = nodeList.item(0).getFirstChild();
					if (node.getNodeValue() != null){
						String rating = node.getNodeValue().split(" ")[0];
						pathRatingMap.put(filepath, Double.parseDouble(rating));
					} else {
						pathRatingMap.put(filepath, 0.0);
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
		return pathRatingMap;
	}
}
