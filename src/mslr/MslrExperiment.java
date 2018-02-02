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

    final static int RELEVANCE_THRESHOLD = 1;

    public static void main(String[] args) {
	String querysetPath = args[0];
	int partitionCount = 50;
	System.out.println("Loading sorted click counts..");
	List<Integer> clickCountsList = loadSortedClickCountList(querysetPath);
	System.out.println("Making thresholds list..");
	List<Integer> thresholdList = buildThresholdsList(clickCountsList,
		partitionCount);
	System.out.println("Thresholds: " + thresholdList);
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
	try (FileWriter fwPre = new FileWriter("pre20.csv");
		FileWriter fwRecall = new FileWriter("recall.csv");
		FileWriter fwMRR = new FileWriter("mrr.csv");
		FileWriter fwMAP = new FileWriter("map.csv");
		FileWriter fwNDCG = new FileWriter("ndcg.csv")) {
	    for (int qid : qidResultMap.keySet()) {
		StringBuilder preStringBuilder = new StringBuilder();
		StringBuilder recStringBuilder = new StringBuilder();
		StringBuilder mrrStringBuilder = new StringBuilder();
		StringBuilder mapStringBuilder = new StringBuilder();
		StringBuilder ndcgStringBuilder = new StringBuilder();
		preStringBuilder.append(qid);
		recStringBuilder.append(qid);
		mrrStringBuilder.append(qid);
		mapStringBuilder.append(qid);
		ndcgStringBuilder.append(qid);
		List<QueryResult> results = qidResultMap.get(qid);
		int rels = (int) results.stream()
			.filter(r -> r.rel > RELEVANCE_THRESHOLD).count();
		for (int threshold : thresholdList) {
		    List<QueryResult> filteredResults = results.stream()
			    .filter(result -> result.clickCount >= threshold)
			    .collect(Collectors.toList());
		    preStringBuilder
			    .append("," + precisionAtK(filteredResults, 20));
		    recStringBuilder
			    .append("," + recall(filteredResults, rels));
		    mrrStringBuilder.append("," + mrr(filteredResults));
		    mapStringBuilder
			    .append("," + averagePrecision(filteredResults));
		    ndcgStringBuilder.append("," + ndcg(filteredResults, 20));
		}
		fwPre.write(preStringBuilder.toString() + "\n");
		fwRecall.write(recStringBuilder.toString() + "\n");
		fwMRR.write(mrrStringBuilder.toString() + "\n");
		fwMAP.write(mapStringBuilder.toString() + "\n");
		fwNDCG.write(ndcgStringBuilder.toString() + "\n");
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
		Integer queryClickCount = Integer
			.parseInt(getValueOfKeyValueString(fields[135]));
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
	    if (counter++ >= k) {
		break;
	    }
	    if (result.rel > RELEVANCE_THRESHOLD) {
		tp++;
	    }
	}
	return tp / k;
    }

    protected static double recall(List<QueryResult> results, int rels) {
	if (rels == 0)
	    return 0;
	long tp = results.stream().filter(r -> r.rel > RELEVANCE_THRESHOLD)
		.count();
	return ((double) tp) / rels;
    }

    protected static double mrr(List<QueryResult> results) {
	for (int i = 0; i < results.size(); i++) {
	    if (results.get(i).rel > RELEVANCE_THRESHOLD) {
		return (1.0 / (i + 1));
	    }
	}
	return 0;
    }

    protected static double averagePrecision(List<QueryResult> results) {
	double rels = 0;
	if (rels == 0)
	    return 0;
	double sum = 0;
	for (int i = 0; i < results.size(); i++) {
	    if (results.get(i).rel > RELEVANCE_THRESHOLD) {
		rels++;
		sum += rels / (i + 1);
	    }
	}
	return sum / rels;
    }

    protected static double ndcg(List<QueryResult> results, int p) {
	double dcg = 0;
	for (int i = 0; i < Math.min(p, results.size()); i++) {
	    if (results.get(i).rel > RELEVANCE_THRESHOLD) {
		dcg += results.get(i).rel / (Math.log(i + 2) / Math.log(2));
	    }
	}
	double idcg = idcg(results, p);
	if (idcg == 0)
	    return 0;
	return dcg / idcg;
    }

    protected static double idcg(List<QueryResult> results, int p) {
	List<Double> qrelScoreList = results.stream().map(r -> r.bm25)
		.collect(Collectors.toList());
	Collections.sort(qrelScoreList, Collections.reverseOrder());
	double dcg = 0;
	for (int i = 0; i < Math.min(qrelScoreList.size(), p); i++) {
	    dcg += qrelScoreList.get(i) / (Math.log(i + 2) / Math.log(2));
	}
	return dcg;
    }

    protected static List<Integer> buildThresholdsList(
	    List<Integer> clickCounts, int partitionCount) {
	if (partitionCount > clickCounts.size()) {
	    System.err.println("Partition size is larger than clickCounts!");
	    return clickCounts;
	}
	List<Integer> thresholds = new ArrayList<Integer>();
	int step = (clickCounts.size() + 1) / partitionCount;
	for (int i = 1; i < partitionCount; i++) {
	    thresholds.add(clickCounts.get(i * step - 1));
	}
	thresholds.add(clickCounts.get(clickCounts.size() - 1));
	return thresholds;
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
	    if (o1.bm25 > o2.bm25)
		return -1;
	    else if (o1.bm25 < o2.bm25)
		return 1;
	    return 0;
	}
    }
}
