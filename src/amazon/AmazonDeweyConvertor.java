package amazon;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AmazonDeweyConvertor {

	private static final Logger LOGGER = Logger.getLogger(AmazonDeweyConvertor.class.getName());
	
	private static AmazonDeweyConvertor singletonInstance;
	
	protected static String mapPath = AmazonDirectoryInfo.DEWEY_DICT;
	
	private Map<String, String> deweyToCategory;
	
	private AmazonDeweyConvertor() {
		deweyToCategory = loadDeweyMap(mapPath);
	}

	public static AmazonDeweyConvertor getInstance() {
		if (singletonInstance == null)
			singletonInstance = new AmazonDeweyConvertor();
		return singletonInstance;
	}

	private static Map<String, String> loadDeweyMap(String path) {
		LOGGER.log(Level.INFO, "Loading Dewey dictionary..");
		Map<String, String> deweyMap = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			String line = br.readLine();
			while (line != null) {
				String[] fields = line.split("   ");
				// adds dewey id --> text category
				deweyMap.put(fields[0].trim(), fields[1].trim());
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO, "Dewey dictionary loaded. Size: " + deweyMap.size());
		return deweyMap;
	}

	public String convertDeweyToCategory(String dewey) {
		String text = "";
		if (dewey.contains("."))
			dewey = dewey.substring(0, dewey.indexOf('.'));
		if (deweyToCategory.containsKey(dewey.trim())) {
			text = deweyToCategory.get(dewey.trim());
		}
		return text;
	}
}
