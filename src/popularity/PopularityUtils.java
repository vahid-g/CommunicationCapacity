package popularity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PopularityUtils {

	private static final Logger LOGGER = Logger.getLogger(PopularityUtils.class.getName());

	public static Map<String, Double> loadIdPopularityMap(String pathRatePath) {
		Map<String, Double> idPopMap = new HashMap<String, Double>();
		try {
			for (String line : Files.readAllLines(Paths.get(pathRatePath))) {
					Pattern ptr = Pattern.compile(".+/([^.]+).(xml|txt), ?([^,]+)(,.+)?");
					Matcher matcher = ptr.matcher(line);
					if (matcher.find()) {
						String id = matcher.group(1);
						Double pop = Double.parseDouble(matcher.group(3));
						idPopMap.put(id,  pop);
					} else {
						LOGGER.log(Level.WARNING, "Couldn't parse line: " + line);
					}
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return idPopMap;
	}

}
