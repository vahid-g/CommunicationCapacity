package data_processing;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MergeCountTitle {

	public static void main(String[] args) {
//		Path countFilePath = Paths.get("/scratch/data-sets/small_counts");
//		Path dataFilePath = Paths.get("/scratch/data-sets/grep_1.out");
		Path countFilePath = Paths.get("/scratch/cluster-share/ghadakcv/"
				+ args[0]);
		Path dataFilePath = Paths.get("/scratch/cluster-share/ghadakcv/"
				+ args[1]);
		List<String> titles;
		try {
			titles = Files.readAllLines(dataFilePath, Charset.forName("UTF-8"));
			Map<String, Integer> countMap = new HashMap<String, Integer>();
			for (String title : titles) {
				countMap.put(title, 0);
			}
			try (BufferedReader br = Files.newBufferedReader(countFilePath,
					Charset.forName("UTF-8"))) {
				String line = null;
				while ((line = br.readLine()) != null) {
					String[] fields = line.split(" ");
					String title = fields[0].replace("_", " ");
					if (countMap.containsKey(title)) {
						Integer oldFreq = countMap.get(title);
						Integer newFreq = Integer.parseInt(fields[1]);
						countMap.put(title, oldFreq + newFreq);
						// System.out.println("found key: " + title + " freq: "
						// + newFreq);
					} else {
						System.err.println("missing key: " + title);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
				System.out.println(entry.getKey() + ", " + entry.getValue());
			}

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}
