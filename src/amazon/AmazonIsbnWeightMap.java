package amazon;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AmazonIsbnWeightMap {
	
	public static void main(String[] args) {
		String w = AmazonIsbnWeightMap.getInstance("data/amazon/path_pop/amazon_path_reviews.csv")
				.getWeight("0446521000");
		System.out.println(w);
		
	}

	private static final Logger LOGGER = Logger.getLogger(AmazonIsbnWeightMap.class.getName());
	private static AmazonIsbnWeightMap singletonInstance;

	private Map<String, String> isbnToLtidMap = null;

	public static AmazonIsbnWeightMap getInstance(String isbnDictPath) {
		if (singletonInstance == null)
			singletonInstance = new AmazonIsbnWeightMap(isbnDictPath);
		return singletonInstance;
	}
	
	private AmazonIsbnWeightMap(String isbnDictPath) {
		isbnToLtidMap = loadIsbnWeightMap(isbnDictPath);
	}

	public static Map<String, String> loadIsbnWeightMap(String path) {
		LOGGER.log(Level.INFO, "Loading Isbn -> Weight map..");
		Map<String, String> isbnToWeight = new HashMap<String, String>();
		try {
			for (String line : Files.readAllLines(Paths.get(path))) {
				String[] ids = line.split(",");
				String isbn = ids[0].substring(ids[0].lastIndexOf('/') + 1, ids[0].indexOf('.'));
				isbnToWeight.put(isbn, ids[1]);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO, "Isbn -> Weight map size: " + isbnToWeight.size());
		return isbnToWeight;
	}

	public String getWeight(String isbn) {
		return isbnToLtidMap.get(isbn);
	}

}
