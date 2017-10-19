package amazon;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AmazonIsbnConverter {

	private static final Logger LOGGER = Logger
			.getLogger(AmazonIsbnConverter.class.getName());
	private static Map<String, String> isbnToLtidMap = null;
	private static Map<String, Set<String>> ltidToIsbnMap = null;

	public static Map<String, String> loadIsbnToLtidMap(String path) {
		if (isbnToLtidMap == null){
			buildMaps(path);
		}
		return isbnToLtidMap;
	}
	
	public static Map<String, Set<String>> loadLtidToIsbnMap (String path) {
		if (ltidToIsbnMap == null) {
			buildMaps(path);
		}
		return ltidToIsbnMap;
	}
	
	private static void buildMaps(String path) {
		LOGGER.log(Level.INFO, "Loading ISBNs and Ltid conversion maps..");
		isbnToLtidMap = new HashMap<String, String>();
		ltidToIsbnMap = new HashMap<String, Set<String>>();
		try {
			for (String line : Files.readAllLines(Paths.get(path))) {
				String[] ids = line.split(",");
				isbnToLtidMap.put(ids[0], ids[1]);
				Set<String> isbns = ltidToIsbnMap.get(ids[1]);
				if (isbns == null) {
					isbns = new HashSet<String>();
				}
				isbns.add(ids[0]);
				ltidToIsbnMap.put(ids[1], isbns);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO,
				"ISBNs -> Ltid map size: " + isbnToLtidMap.size());
		LOGGER.log(Level.INFO,
				"Ltid -> ISBNs map size: " + ltidToIsbnMap.size());

	}

	public String convertIsbnToLtid(String isbn) {
		return isbnToLtidMap.get(isbn);
	}

}
