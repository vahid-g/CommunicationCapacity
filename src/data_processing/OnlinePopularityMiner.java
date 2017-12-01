package data_processing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import wiki09.ClusterDirectoryInfo;

public class OnlinePopularityMiner {

    public static void main(String[] args) {
	String datasetPath = "/scratch/data-sets/imdb/imdb-inex/movies";
	// String datasetPath = "/data/imdb-inex/movies";
	Collection<File> allFiles = FileUtils.listFiles(new File(datasetPath),
		null, true);
	try (FileWriter fw = new FileWriter("data/file-count.csv")) {
	    for (File file : allFiles) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
		    byte[] data = new byte[(int) file.length()];
		    fis.read(data);
		    String fileContent = new String(data, "UTF-8");
		    Pattern p = Pattern.compile("<url>([^<]+)</url>");
		    Matcher m = p.matcher(fileContent);
		    if (m.find()) {
			String rawUrl = m.group(1);
			String url = rawUrl.replace("\n", "").replace("\r", "")
				.trim().replaceAll(" ", "%20");
			fw.write(file.getPath() + "," + extractImdbRating(url)
				+ "\n");
			fw.flush();
		    } else {
			System.err.println("no url in file: " + file.getName());
		    }

		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	} catch (IOException e1) {
	    e1.printStackTrace();
	}
    }

    public static String extractImdbRating(String url) {
	String content = "";
	content = getHTML(url);
	Pattern p = Pattern
		.compile("<span itemprop=\"ratingValue\">([^<]*)</span>");
	Matcher m = p.matcher(content);
	if (m.find()) {
	    return m.group(1);
	} else {
	    return "-1";
	}
    }

    public static void extractWikiCounts() {
	Collection<File> allFiles = FileUtils.listFiles(new File(
		ClusterDirectoryInfo.DATASET09_PATH), null, true);
	try (FileWriter fw = new FileWriter(
		ClusterDirectoryInfo.PATH_COUNT_FILE09)) {
	    for (File file : allFiles) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
		    byte[] data = new byte[(int) file.length()];
		    fis.read(data);
		    String fileContent = new String(data, "UTF-8");
		    int length = fileContent.length() > 8 ? 8 : fileContent
			    .length();
		    if (fileContent.substring(0, length).equals("REDIRECT")) {
			return;
		    }
		    Pattern p = Pattern.compile(".*<title>(.*?)</title>.*",
			    Pattern.DOTALL);
		    Matcher m = p.matcher(fileContent);
		    m.find();
		    String title = "";
		    if (m.matches())
			title = m.group(1);
		    else
			System.out.println("!!! title not found in "
				+ file.getName());
		    int pageCount = extractSinleCount(title);
		    fw.write(file.getName() + ", " + pageCount);
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	} catch (IOException e1) {
	    e1.printStackTrace();
	}
    }

    public static int extractSinleCount(String name) {
	String template = "http://stats.grok.se/json/en/200901/";
	String url = template
		+ name.replace("\n", "").replace("\r", "").trim()
			.replaceAll(" ", "%20");
	System.out.println(url);
	String jsonString = "";
	try {
	    jsonString = getHTML(url);
	} catch (Exception e) {
	    System.err.println("Get request failed for url: " + url);
	    return 0;
	}
	JSONObject pageJson = new JSONObject(jsonString);
	JSONObject dailyViews = pageJson.getJSONObject("daily_views");
	Iterator<String> keys = dailyViews.keys();
	int sum = 0;
	while (keys.hasNext()) {
	    // System.out.println(dailyViews.get(keys.next()));
	    sum += Integer.parseInt(dailyViews.get(keys.next()).toString());
	}
	return sum;
    }

    private static String getHTML(String urlToRead) {
	StringBuilder resultBuilder = new StringBuilder();
	System.out.println("getting: " + urlToRead);
	try {
	    URL url = new URL(urlToRead);
	    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    conn.setRequestMethod("GET");
	    conn.setRequestProperty(
		    "User-Agent",
		    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
	    BufferedReader rd = new BufferedReader(new InputStreamReader(
		    conn.getInputStream()));
	    String line;
	    while ((line = rd.readLine()) != null) {
		resultBuilder.append(line);
	    }
	    rd.close();
	} catch (Exception e) {
	    System.err.println(e.toString());
	}
	String result = resultBuilder.toString();
	String newUrl = urlToRead.replaceAll("\\{.*\\}", "")
		.replaceAll("%20", " ").trim().replaceAll(" ", "%20");
	if (result.length() == 0 && !newUrl.equals(urlToRead)) {
	    result = getHTML(newUrl);
	}
	return result;
    }
}
