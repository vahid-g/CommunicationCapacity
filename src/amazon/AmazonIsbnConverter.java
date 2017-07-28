package amazon;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AmazonIsbnConverter {

	private static final Logger LOGGER = Logger.getLogger(AmazonIsbnConverter.class.getName());
	private static AmazonIsbnConverter singletonInstance;

	private Map<String, String> isbnToLtidMap = null;

	public static AmazonIsbnConverter getInstance(String isbnDictPath) {
		if (singletonInstance == null)
			singletonInstance = new AmazonIsbnConverter(isbnDictPath);
		return singletonInstance;
	}
	
	private AmazonIsbnConverter(String isbnDictPath) {
		isbnToLtidMap = loadIsbnLtidMap(isbnDictPath);
	}

	public static Map<String, String> loadIsbnLtidMap(String path) {
		LOGGER.log(Level.INFO, "Loading Isbn -> Ltid map..");
		Map<String, String> isbnToLtid = new HashMap<String, String>();
		try {
			for (String line : Files.readAllLines(Paths.get(path))) {
				String[] ids = line.split(",");
				isbnToLtid.put(ids[0], ids[1]);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO, "Isbn -> Ltid map size: " + isbnToLtid.size());
		return isbnToLtid;

	}

	public String convertIsbnToLtid(String isbn) {
		return isbnToLtidMap.get(isbn);
	}

}
