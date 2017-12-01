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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MergeCountTitle {

    public static void main(String[] args) {
	Path countFilePath = Paths.get("/scratch/cluster-share/ghadakcv/"
		+ args[0]);
	// contains title and counts
	Path pathToTitleFile = Paths.get("/scratch/cluster-share/ghadakcv/"
		+ args[1]);
	// coutains filepath to title in wiki13 inex dataset
	List<String> pathTitles;
	try {
	    pathTitles = Files.readAllLines(pathToTitleFile,
		    Charset.forName("UTF-8"));
	    Map<String, Integer> pathToCountMap = new HashMap<String, Integer>();
	    Map<String, String> pathToTitleMap = new HashMap<String, String>();
	    Map<String, String> titlePathMap = new HashMap<String, String>();
	    for (String pathTitle : pathTitles) {
		Pattern pat = Pattern
			.compile("../(inex_13/[0-9[a-f]/]+.xml):(.*)");
		Matcher mat = pat.matcher(pathTitle);
		if (mat.find()) {
		    try {
			String path = mat.group(1);
			String title = mat.group(2);
			pathToCountMap.put(path, 0);
			pathToTitleMap.put(path, title);
			titlePathMap.put(title.trim(), path);
		    } catch (IllegalStateException e1) {
			e1.printStackTrace();
		    } catch (IndexOutOfBoundsException e2) {
			e2.printStackTrace();
		    }
		} else {
		    System.err.println("couldn't find: " + pathTitle);
		}
	    }
	    System.err.println("init file size: " + pathTitles.size());
	    System.err.println("map size:" + pathToCountMap.size());
	    try (BufferedReader br = Files.newBufferedReader(countFilePath,
		    Charset.forName("ISO-8859-1"))) {
		String line = br.readLine();
		do {
		    Pattern pat = Pattern.compile("en (.+) (\\d+) \\d+");
		    Matcher mat = pat.matcher(line);
		    if (mat.find()) {
			try {
			    String title = mat.group(1).replace("_", " ");
			    String count = mat.group(2);
			    if (titlePathMap.containsKey(title)) {
				String path = titlePathMap.get(title);
				Integer oldFreq = pathToCountMap.get(path);
				Integer newFreq = Integer.parseInt(count);
				pathToCountMap.put(path, oldFreq + newFreq);
			    } else {
				// System.err.println("missing key: " + title);
			    }
			} catch (IllegalStateException e1) {
			    e1.printStackTrace();
			} catch (IndexOutOfBoundsException e2) {
			    e2.printStackTrace();
			}
		    }
		    line = br.readLine();
		} while (line != null);
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	    for (Map.Entry<String, Integer> entry : pathToCountMap.entrySet()) {
		System.out.println(entry.getKey() + ", " + entry.getValue()
			+ ", " + pathToTitleMap.get(entry.getKey()));
	    }
	} catch (IOException e1) {
	    e1.printStackTrace();
	}
    }

}
