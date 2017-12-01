package amazon.popularity;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AmazonPopularityMiner {

    public static void main(String[] args) throws IOException {
	String isbnsFile = "data/amazon/all_isbns";
	try (FileWriter fw = new FileWriter("amazon_books_metadata.csv")) {
	    for (String isbn : Files.readAllLines(Paths.get(isbnsFile))) {
		isbn = " 0060508116";
		String url = "https://www.amazon.com/dp/" + isbn;
		String content = getHTML(url);
		System.out.println(content);
		String rank = extractFieldFromAmazonHtml(content,
			"#([0-9,]+) in Books");
		System.out.println(rank);
		String revCount = extractFieldFromAmazonHtml(content,
			"([0-9,]+) costumer reviews?");
		String rev = extractFieldFromAmazonHtml(content,
			"([0-9.]+) out of 5 stars");
		fw.write(isbn + "," + rank + "," + revCount + "," + rev + "\n");
		break;
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    private static String extractFieldFromAmazonHtml(String content,
	    String regexPattern) {
	Pattern pattern = Pattern.compile(regexPattern);
	Matcher matcher = pattern.matcher(content);
	String value = "0";
	if (matcher.find()) {
	    value = matcher.group(1).replace(",", "");
	}
	return value;
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
	return result;
    }

}
