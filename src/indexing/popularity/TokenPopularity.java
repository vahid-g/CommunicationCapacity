package indexing.popularity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TokenPopularity {
	
	public static final Logger LOGGER = Logger.getLogger(TokenPopularity.class.getName());
	public double mean;
	public double min;

	public TokenPopularity(double mean, double min) {
		this.mean = mean;
		this.min = min;
	}

	public static Map<String, TokenPopularity> loadTokenPopularities(String indexFile) throws IOException {
		Map<String, TokenPopularity> map = new HashMap<String, TokenPopularity>();
		try (BufferedReader br = new BufferedReader(new FileReader(indexFile))) {
			LOGGER.log(Level.INFO, "loading pop index " + indexFile);
			String line;
			while ((line = br.readLine()) != null) {
				String[] field = line.split(",");
				int minFieldIndex = field.length - 1;
				int meanFieldIndex = field.length - 2;
				if (map.containsKey(field[0])) {
					double min = Math.min(Double.parseDouble(field[minFieldIndex]), map.get(field[0]).min);
					map.put(field[0], new TokenPopularity(Double.parseDouble(field[meanFieldIndex]), min));
				} else {
					map.put(field[0], new TokenPopularity(Double.parseDouble(field[meanFieldIndex]),
							Double.parseDouble(field[minFieldIndex])));
				}
			}
			LOGGER.log(Level.INFO, "loading pop index done!");
			return map;
		}
	}
}