package wiki13.cache_selection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TokenPopularity {
	public double mean;
	public double min;

	public TokenPopularity(double mean, double min) {
		this.mean = mean;
		this.min = min;
	}

	static Map<String, TokenPopularity> loadTokenPopularities(String indexFile) throws IOException {
		Map<String, TokenPopularity> map = new HashMap<String, TokenPopularity>();
		try (BufferedReader br = new BufferedReader(new FileReader(indexFile))) {
			String[] field = br.readLine().split(",");
			if (field.length > 3) {
				throw new IOException();
			}
			map.put(field[0], new TokenPopularity(Double.parseDouble(field[1]), Double.parseDouble(field[2])));
		}
		return map;
	}
}