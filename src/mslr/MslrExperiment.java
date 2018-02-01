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
	int partitionCount = 10;
	List<Integer> clickCountsList = loadSortedClickCountList("test_data/mslr_sample.txt");
	Map<Integer, List<MslrQueryResult>> qidResultMap = null;
	try (BufferedReader br = new BufferedReader(new FileReader(""))) {
	    qidResultMap = loadQueryResults(br);
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	try (FileWriter fw = new FileWriter("")) {
	    for (int qid : qidResultMap.keySet()) {
		StringBuilder sb = new StringBuilder();
		sb.append(qid + ",");
		for (double i = 1; i <= partitionCount; i++) {
		    double partitionPercentage = i / partitionCount;
		    int threshold = clickCountsList.get((int) Math.floor(partitionPercentage * clickCountsList.size()));
		    List<MslrQueryResult> results = qidResultMap.get(qid);
		    results.stream().filter(result -> result.clickCount < threshold).collect(Collectors.toList());
		    double pk = precisionAtK(results, 10);
		    sb.append(pk + ",");
		}
		fw.write(sb.toString() + "\n");
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }

    private static double precisionAtK(List<MslrQueryResult> results, int k) {
	int counter = 0;
	double tp = 0;
	for (MslrQueryResult result : results) {
	    if (counter >= k) {
		break;
	    }
	    if (result.rel > 0) { // TODO this threshold should be set
		tp++;
	    }
	}
	return tp / k;
    }

    private static Map<Integer, List<MslrQueryResult>> loadQueryResults(BufferedReader br) throws IOException {
	Map<Integer, List<MslrQueryResult>> qidResultMap = new HashMap<Integer, List<MslrQueryResult>>();
	String line;
	int previousQid = -1;
	List<MslrQueryResult> results = new ArrayList<MslrQueryResult>();
	while ((line = br.readLine()) != null) {
	    String fields[] = line.split(" ");
	    int rel = Integer.parseInt(fields[0]);
	    int qid = Integer.parseInt(fields[1]);
	    double bm = Double.parseDouble(fields[107]);
	    int cc = Integer.parseInt(fields[136]);
	    if (qid != previousQid && previousQid != -1) {
		Collections.sort(results, new Comparator<MslrQueryResult>() {
		    @Override
		    public int compare(MslrQueryResult o1, MslrQueryResult o2) {
			return (o1.bm25 > o2.bm25) ? 1 : 0;
		    }
		});
		qidResultMap.put(previousQid, results);
		results = new ArrayList<MslrQueryResult>();
	    }
	    results.add(new MslrQueryResult(bm, rel, cc));
	    previousQid = qid;
	}
	return qidResultMap;
    }

    static class MslrQueryResult {
	double bm25;
	int rel;
	int clickCount;

	public MslrQueryResult(double bm25, int rel, int clickCount) {
	    this.bm25 = bm25;
	    this.rel = rel;
	    this.clickCount = clickCount;
	}
    }

    public static List<Integer> parseQueries() {
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
		    System.err.println("Not enough fields for line: " + urlIdCounter);
		    continue;
		}
		urlIdCounter++;
		List<String> fieldsList = new ArrayList<String>();
		fieldsList.add(fields[127]);
		fieldsList.add(fields[128]);
		fieldsList.add(fields[129]);
		fieldsList.add(fields[130]);
		String urlID = fieldsList.stream().map(x -> x.substring(x.indexOf(':') + 1))
			.collect(Collectors.joining(" "));
		Integer clickCount = Integer.parseInt(fields[136].substring(fields[136].indexOf(':') + 1));
		if (urlClickCountMap.containsKey(urlID)) {
		    urlClickCountMap.put(urlID, urlClickCountMap.get(urlID) + clickCount);
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

    public static List<Integer> loadSortedClickCountList(String dataFile) {
	int urlIdCounter = 0;
	List<Integer> clickCounts = new ArrayList<Integer>();
	try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
	    Map<String, Integer> urlClickCountMap = new HashMap<String, Integer>();
	    String line;
	    System.out.println("Building URL -> ClickCount Map");
	    while ((line = br.readLine()) != null) {
		String[] fields = line.split(" ");
		if (fields.length < 138) {
		    System.err.println("Not enough fields for line: " + urlIdCounter);
		    continue;
		}
		urlIdCounter++;
		List<String> fieldsList = new ArrayList<String>();
		fieldsList.add(fields[127]);
		fieldsList.add(fields[128]);
		fieldsList.add(fields[129]);
		fieldsList.add(fields[130]);
		String urlID = fieldsList.stream().map(x -> x.substring(x.indexOf(':') + 1))
			.collect(Collectors.joining(" "));
		Integer clickCount = Integer.parseInt(fields[136].substring(fields[136].indexOf(':') + 1));
		if (urlClickCountMap.containsKey(urlID)) {
		    continue;
		    // urlClickCountMap.put(urlID,
		    // urlClickCountMap.get(urlID) + clickCount);
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
