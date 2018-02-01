package mslr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MslrExperiment {

    final static int MAX_PAGECOUNT = 2789632;

    public static void main(String[] args) {
	String querysetPath = args[1];
	int partitionCount = 10;
	System.out.println("Loading sorted click counts..");
	List<Integer> clickCountsList = loadSortedClickCountList(querysetPath);
	System.out.println("Loading query results..");
	Map<Integer, List<QueryResult>> qidResultMap = null;
	try (BufferedReader br = new BufferedReader(
		new FileReader(querysetPath))) {
	    qidResultMap = loadQueryResults(br);
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	System.out.println("Computing query results..");
	try (FileWriter fw = new FileWriter("mslr_result.csv")) {
	    for (int qid : qidResultMap.keySet()) {
		StringBuilder sb = new StringBuilder();
		sb.append(qid);
		for (double i = 1; i <= partitionCount; i++) {
		    double partitionPercentage = i / partitionCount;
		    int threshold = clickCountsList.get((int) Math
			    .ceil(partitionPercentage * clickCountsList.size())
			    - 1);
		    System.out.println(threshold);
		    List<QueryResult> results = qidResultMap.get(qid);
		    results = results.stream()
			    .filter(result -> result.clickCount >= threshold)
			    .collect(Collectors.toList());
		    double pk = precisionAtK(results, 10);
		    sb.append("," + pk);
		}
		fw.write(sb.toString() + "\n");
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }

    // loads the click counts of URLs (trying to match duplicate URLs) and sorts
    // them
    protected static List<Integer> loadSortedClickCountList(String dataFile) {
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
			.map(x -> getValueOfKeyValueString(x))
			.collect(Collectors.joining(" "));
		Integer clickCount = Integer
			.parseInt(getValueOfKeyValueString(fields[136]));
		if (urlClickCountMap.containsKey(urlID)) {
		    continue;
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

    // loads a map of QIDs to MslrQueryResult list
    protected static Map<Integer, List<QueryResult>> loadQueryResults(
	    BufferedReader br) throws IOException {
	Map<Integer, List<QueryResult>> qidResultMap = new HashMap<Integer, List<QueryResult>>();
	String line;
	int previousQid = -1;
	List<QueryResult> results = new ArrayList<QueryResult>();
	while ((line = br.readLine()) != null) {
	    String fields[] = line.split(" ");
	    int rel = Integer.parseInt(getValueOfKeyValueString(fields[0]));
	    int qid = Integer.parseInt(getValueOfKeyValueString(fields[1]));
	    double bm = Double
		    .parseDouble(getValueOfKeyValueString(fields[107]));
	    int cc = Integer.parseInt(getValueOfKeyValueString(fields[136]));
	    if (qid != previousQid && previousQid != -1) {
		Collections.sort(results, new BM25Comparator());
		qidResultMap.put(previousQid, results);
		results = new ArrayList<QueryResult>();
	    }
	    results.add(new QueryResult(bm, rel, cc));
	    previousQid = qid;
	}
	Collections.sort(results, new BM25Comparator());
	qidResultMap.put(previousQid, results);
	return qidResultMap;
    }

    protected static String getValueOfKeyValueString(String keyValue) {
	return keyValue.substring(keyValue.indexOf(':') + 1);
    }

    protected static double precisionAtK(List<QueryResult> results, int k) {
	int counter = 0;
	double tp = 0;
	for (QueryResult result : results) {
	    if (counter >= k) {
		break;
	    }
	    if (result.rel > 0) { // TODO this threshold should be set
		tp++;
	    }
	}
	return tp / k;
    }

    protected static List<Integer> parseQueries(String dataFile) {
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
			fields[136].substring(fields[136].indexOf(':') + 1));
		if (urlClickCountMap.containsKey(urlID)) {
		    urlClickCountMap.put(urlID,
			    urlClickCountMap.get(urlID) + clickCount);
		} else {
		    // urlClickCountMap.put(urlID, clickCount);
		    continue;
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

    protected static class QueryResult {
	double bm25;
	int rel;
	int clickCount;

	public QueryResult(double bm25, int rel, int clickCount) {
	    this.bm25 = bm25;
	    this.rel = rel;
	    this.clickCount = clickCount;
	}
    }

    protected static class BM25Comparator implements Comparator<QueryResult> {
	@Override
	public int compare(QueryResult o1, QueryResult o2) {
	    return (o1.bm25 > o2.bm25) ? 1 : 0;
	}
    }
}
