package amazon.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AmazonIsbnConverter {
	
	private static final Logger LOGGER = Logger.getLogger(AmazonIsbnConverter.class.getName());
	private static AmazonIsbnConverter singletonInstance;
	protected static String deweyCategoryDictPath;
	
	private Map<String, String> isbnToLtidMap = null;
	private AmazonIsbnConverter(String isbnDictPath) {
		isbnToLtidMap = loadIsbnLtidMap(isbnDictPath);
	}

	public static AmazonIsbnConverter getInstance(String isbnDictPath) {
		if (singletonInstance == null)
			singletonInstance = new AmazonIsbnConverter(isbnDictPath);
		return singletonInstance;
	}

	public static Map<String, String> loadIsbnLtidMap(String path) {
		AmazonPopularityUtils.LOGGER.log(Level.INFO, "Loading Isbn -> Ltid map..");
		Map<String, String> isbnToLtid = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			String line = br.readLine();
			while (line != null) {
				String[] ids = line.split(",");
				isbnToLtid.put(ids[0], ids[1]);
				line = br.readLine();
			}
			AmazonPopularityUtils.LOGGER.log(Level.INFO, "Isbn -> Ltid map size: " + isbnToLtid.size());
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} 
		return isbnToLtid;
	}
	
	public String convertIsbnToLtid(String isbn) {
		return isbnToLtidMap.get(isbn);
	}

}
