package popularity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PopularityUtils {

	private static final Logger LOGGER = Logger.getLogger(PopularityUtils.class.getName());

	public static Map<String, String> loadIsbnRatingsMap(String pathRatePath) {
		Map<String, String> isbnRateMap = new HashMap<String, String>();
		try {
			for (String line : Files.readAllLines(Paths.get(pathRatePath))) {
				String[] fields = line.split(",");
				String path = fields[0];
				String isbn = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.'));
				String score = fields[2];
				isbnRateMap.put(isbn, score);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return isbnRateMap;
	}

}
