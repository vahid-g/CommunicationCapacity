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
		// Path countFilePath = Paths.get("/scratch/data-sets/part00");
		// Path dataFilePath = Paths.get("/scratch/data-sets/grep_00.out");
		Path countFilePath = Paths.get("/scratch/cluster-share/ghadakcv/" + args[0]);
		Path dataFilePath = Paths.get("/scratch/cluster-share/ghadakcv/" + args[1]);
		// Path countFilePath = Paths.get("part00");
		// Path dataFilePath = Paths.get("grep.txt");
		List<String> pathTitles;
		try {
			pathTitles = Files.readAllLines(dataFilePath, Charset.forName("UTF-8"));
			Map<String, Integer> countMap = new HashMap<String, Integer>();
			Map<String, String> titlePathMap = new HashMap<String, String>();
			for (String pt : pathTitles) {
				if (!pt.contains(";")) {
					continue;
				}
				String[] pair = pt.split(";");
				String title = pair[1];
				String path = pair[0];
				titlePathMap.put(title.trim(), path);
				countMap.put(path, 0);
			}
			try (BufferedReader br = Files.newBufferedReader(countFilePath, Charset.forName("ISO-8859-1"))) {
				String line = br.readLine();
				do {
					String[] fields = line.split(" ");
					String title = fields[0].replace("_", " ");
					if (titlePathMap.containsKey(title)) {
						String path = titlePathMap.get(title);
						Integer oldFreq = countMap.get(path);
						Integer newFreq = Integer.parseInt(fields[1]);
						countMap.put(path, oldFreq + newFreq);
					} else {
						// System.err.println("missing key: " + title);
					}
					line = br.readLine();
				} while (line != null);
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
