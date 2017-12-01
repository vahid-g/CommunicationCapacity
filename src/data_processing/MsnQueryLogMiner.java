package data_processing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MsnQueryLogMiner {

    public static class MsnLogQuery {
	String text = "";
	String id = "";
	String wikiId = "";
	int freq = 0;
	String type = "";
    }

    public static void main(String[] args) {
	String dataset = "/scratch/Dropbox/Research/Database Communication/experiments/queries/msn/raw data/msn_8009.txt";
	try (BufferedReader br = new BufferedReader((new FileReader(dataset)))) {
	    String line = br.readLine();
	    Pattern firstLinePtr = Pattern.compile("# (\\d+) -+");
	    Pattern wikiPtr = Pattern.compile("# wikiID : (.+)");
	    Pattern freqPtr = Pattern.compile("# frequency : (\\d+)");
	    Pattern entityPtr = Pattern.compile("# entityID : .*");
	    MsnLogQuery query = null;
	    List<MsnLogQuery> queries = new ArrayList<MsnLogQuery>();
	    while (line != null) {
		Matcher firstMatcher = firstLinePtr.matcher(line);
		Matcher wikiMatcher = wikiPtr.matcher(line);
		Matcher freqMatcher = freqPtr.matcher(line);
		Matcher entityMatcher = entityPtr.matcher(line);
		if (firstMatcher.find()) {
		    if (query != null) {
			queries.add(query);
		    }
		    query = new MsnLogQuery();
		    query.id = firstMatcher.group(1);
		} else if (wikiMatcher.find()) {
		    query.wikiId = wikiMatcher.group(1);
		} else if (freqMatcher.find()) {
		    query.freq = Integer.parseInt(freqMatcher.group(1).trim());
		} else if (entityMatcher.find()) {
		    query.text = br.readLine();
		}
		line = br.readLine();
	    }
	    queries.add(query);
	    try (FileWriter fw = new FileWriter("msn_queries.csv")) {
		for (MsnLogQuery mlq : queries) {
		    fw.write(mlq.text.replaceAll(",", "") + "," + mlq.freq
			    + "\n");
		}
	    }

	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
}
