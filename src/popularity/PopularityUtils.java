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

	public static Map<String, Double> loadIdPopularityMap(String pathRatePath) {
		Map<String, Double> idPopMap = new HashMap<String, Double>();
		try {
			for (String line : Files.readAllLines(Paths.get(pathRatePath))) {
				String[] fields = line.split(",");
				String path = fields[0];
				String id = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.'));
				Double pop = Double.parseDouble(fields[2]);
				idPopMap.put(id, pop);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return idPopMap;
	}

}
