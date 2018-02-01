package mslr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DatasetMaker {

    final static int MAX_PAGECOUNT = 2789632;

    public static void main(String[] args) {
	int partitionCount = 10;
	List<Integer> clickCountsList = loadSortedClickCountList();
	for (double i = 1; i <= partitionCount; i++) {
	    double partitionPercentage = i / partitionCount; 
	    int threshold = clickCountsList.get((int)Math.floor(partitionPercentage * clickCountsList.size()));
	    
	}
    }

    public static List<Integer> loadSortedClickCountList() {
	String dataFile = "test_data/mslr_sample.txt";
	int urlIdCounter = 0;
	List<Integer> clickCounts = new ArrayList<Integer>();
	try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
	    Map<String, Integer> urlClickCountMap = new HashMap<String, Integer>();
	    String line;
	    System.out.println("Building URL -> ClickCount Map");
	    while ((line = br.readLine()) != null) {
		String[] fields = line.split(" ");
		if (fields.length < 138) {
		    System.err.println(
			    "Not enough fields for line: " + urlIdCounter);
		    continue;
		}
		urlIdCounter++;
		List<String> fieldsList = new ArrayList<String>();
		fieldsList.add(fields[127]);
		fieldsList.add(fields[128]);
		fieldsList.add(fields[129]);
		fieldsList.add(fields[130]);
		String urlID = fieldsList.stream()
			.map(x -> x.substring(x.indexOf(':') + 1))
			.collect(Collectors.joining(" "));
		Integer clickCount = Integer.parseInt(
			fields[136].substring(fields[136].indexOf(':')));
		if (urlClickCountMap.containsKey(urlID)) {
		    urlClickCountMap.put(urlID,
			    urlClickCountMap.get(urlID) + 1);
		} else {
		    urlClickCountMap.put(urlID, clickCount);
		}
	    }

	    System.out.println("Sorting URLs");
	    clickCounts.addAll(urlClickCountMap.values());
	    Collections.sort(clickCounts, Collections.reverseOrder());
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return clickCounts;
    }
}
