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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MslrExperiment {

	final static int MAX_PAGECOUNT = 2789632;

	final static int RELEVANCE_THRESHOLD = 2;

	public static void main(String[] args) {
		String querysetPath = args[0];
		runSizeExperiment(querysetPath);
	}

	public static void runSizeExperiment(String querysetPath) {
		int partitionCount = 50;
		List<URL> urlList = new ArrayList<URL>();
		System.out.println("Loading query results..");
		Map<Integer, List<QueryResult>> qidResultMap = null;
		try (BufferedReader br = new BufferedReader(new FileReader(querysetPath))) {
			qidResultMap = loadQueryResultsWithUrlId(br, urlList);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Building subsets");
		List<Set<String>> urlSetsList = new ArrayList<Set<String>>();
		int step = (urlList.size() + 1) / partitionCount;
		for (int i = 1; i <= partitionCount; i++) {
			Set<String> urls = new HashSet<String>();
			for (int j = 0; j < Math.min(i * step, urlList.size()); j++) {
				urls.add(urlList.get(j).urlId);
			}
			urlSetsList.add(urls);
		}
		System.out.println("Computing query results..");
		try (FileWriter fwPre = new FileWriter("pre20.csv");
				FileWriter fwRecall = new FileWriter("recall.csv");
				FileWriter fwMRR = new FileWriter("mrr.csv");
				FileWriter fwMAP = new FileWriter("map.csv");
				FileWriter fwNDCG = new FileWriter("ndcg.csv");
				FileWriter fwRecallK = new FileWriter("recall_k.csv")) {
			for (int qid : qidResultMap.keySet()) {
				StringBuilder preStringBuilder = new StringBuilder();
				StringBuilder recStringBuilder = new StringBuilder();
				StringBuilder mrrStringBuilder = new StringBuilder();
				StringBuilder mapStringBuilder = new StringBuilder();
				StringBuilder ndcgStringBuilder = new StringBuilder();
				StringBuilder recallKStringBuilder = new StringBuilder();
				preStringBuilder.append(qid);
				recStringBuilder.append(qid);
				mrrStringBuilder.append(qid);
				mapStringBuilder.append(qid);
				ndcgStringBuilder.append(qid);
				recallKStringBuilder.append(qid);
				List<QueryResult> results = qidResultMap.get(qid);
				int rels = (int) results.stream().filter(r -> r.rel > RELEVANCE_THRESHOLD).count();
				for (int i = 0; i < partitionCount; i++) {
					Set<String> subset = urlSetsList.get(i);
					List<QueryResult> filteredResults = results.stream()
							.filter(result -> subset.contains(result.url.urlId)).collect(Collectors.toList());
					preStringBuilder.append("," + precisionAtK(filteredResults, 20));
					recStringBuilder.append("," + recall(filteredResults, rels));
					mrrStringBuilder.append("," + mrr(filteredResults));
					mapStringBuilder.append("," + averagePrecision(filteredResults, rels));
					ndcgStringBuilder.append("," + ndcg(filteredResults, 20));
					recallKStringBuilder.append("," + recallAtK(filteredResults, 30));
				}
				fwPre.write(preStringBuilder.toString() + "\n");
				fwRecall.write(recStringBuilder.toString() + "\n");
				fwMRR.write(mrrStringBuilder.toString() + "\n");
				fwMAP.write(mapStringBuilder.toString() + "\n");
				fwNDCG.write(ndcgStringBuilder.toString() + "\n");
				fwRecallK.write(recallKStringBuilder.toString() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected static Map<Integer, List<QueryResult>> loadQueryResultsWithUrlId(BufferedReader br,
			List<URL> urlClickCountList) throws IOException {
		Map<Integer, List<QueryResult>> qidResultMap = new HashMap<Integer, List<QueryResult>>();
		String line;
		int previousQid = -1;
		List<QueryResult> queryResultList = new ArrayList<QueryResult>();
		Map<String, URL> urlMap = new HashMap<String, URL>();
		while ((line = br.readLine()) != null) {
			String fields[] = line.split(" ");
			String urlStringId = extractUrlId(fields);
			Integer clickCount = Integer.parseInt(getValueOfKeyValueString(fields[136]));
			URL url = urlMap.get(urlStringId);
			if (url == null) {
				url = new URL(urlStringId, clickCount);
				urlMap.put(urlStringId, url);
			}
			int rel = Integer.parseInt(fields[0]);
			int qid = Integer.parseInt(getValueOfKeyValueString(fields[1]));
			double bm = Double.parseDouble(getValueOfKeyValueString(fields[107]));
			if (qid != previousQid && previousQid != -1) {
				Collections.sort(queryResultList, new BM25Comparator());
				qidResultMap.put(previousQid, queryResultList);
				queryResultList = new ArrayList<QueryResult>();
			}
			queryResultList.add(new QueryResult(bm, rel, url));
			previousQid = qid;
		}
		Collections.sort(queryResultList, new BM25Comparator());
		qidResultMap.put(previousQid, queryResultList);
		urlClickCountList.addAll(urlMap.values());
		Collections.sort(urlClickCountList, new Comparator<URL>() {
			@Override
			public int compare(URL o1, URL o2) {
				if (o1.clickCount < o2.clickCount) {
					return +1;
				} else if (o1.clickCount > o2.clickCount) {
					return -1;
				} else {
					return 0;
				}
			}

		});
		return qidResultMap;
	}

	protected static String extractUrlId(String[] fields) {
		List<String> fieldsList = new ArrayList<String>();
		fieldsList.add(fields[127]);
		fieldsList.add(fields[128]);
		fieldsList.add(fields[129]);
		fieldsList.add(fields[130]);
		String urlStringId = fieldsList.stream().map(x -> getValueOfKeyValueString(x)).collect(Collectors.joining("|"));
		return urlStringId;
	}

	public static void runThresholdExperiment(String querysetPath) {
		int partitionCount = 50;
		System.out.println("Loading sorted click counts..");
		List<Integer> clickCountsList = loadSortedClickCountList(querysetPath);
		System.out.println("Making thresholds list..");
		List<Integer> thresholdList = buildThresholdsList(clickCountsList, partitionCount);
		System.out.println("Thresholds: " + thresholdList);
		System.out.println("Loading query results..");
		Map<Integer, List<QueryResult>> qidResultMap = null;
		try (BufferedReader br = new BufferedReader(new FileReader(querysetPath))) {
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
				int rels = (int) results.stream().filter(r -> r.rel > RELEVANCE_THRESHOLD).count();
				for (int threshold : thresholdList) {
					List<QueryResult> filteredResults = results.stream()
							.filter(result -> result.url.clickCount >= threshold).collect(Collectors.toList());
					preStringBuilder.append("," + precisionAtK(filteredResults, 20));
					recStringBuilder.append("," + recall(filteredResults, rels));
					mrrStringBuilder.append("," + mrr(filteredResults));
					mapStringBuilder.append("," + averagePrecision(filteredResults, rels));
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
					System.err.println("Not enough fields for line: " + urlIdCounter);
					continue;
				}
				urlIdCounter++;
				List<String> fieldsList = new ArrayList<String>();
				fieldsList.add(fields[127]);
				fieldsList.add(fields[128]);
				fieldsList.add(fields[129]);
				fieldsList.add(fields[130]);
				String urlID = fieldsList.stream().map(x -> getValueOfKeyValueString(x))
						.collect(Collectors.joining(" "));
				// Integer queryClickCount =
				// Integer.parseInt(getValueOfKeyValueString(fields[135]));
				Integer clickCount = Integer.parseInt(getValueOfKeyValueString(fields[136]));
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
	protected static Map<Integer, List<QueryResult>> loadQueryResults(BufferedReader br) throws IOException {
		Map<Integer, List<QueryResult>> qidResultMap = new HashMap<Integer, List<QueryResult>>();
		String line;
		int previousQid = -1;
		List<QueryResult> results = new ArrayList<QueryResult>();
		while ((line = br.readLine()) != null) {
			String fields[] = line.split(" ");
			int rel = Integer.parseInt(getValueOfKeyValueString(fields[0]));
			int qid = Integer.parseInt(getValueOfKeyValueString(fields[1]));
			double bm = Double.parseDouble(getValueOfKeyValueString(fields[107]));
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
		long tp = results.stream().filter(r -> r.rel > RELEVANCE_THRESHOLD).count();
		return ((double) tp) / rels;
	}

	protected static double recallAtK(List<QueryResult> results, int k) {
		if (results.size() == 0)
			return 0;
		List<QueryResult> resultsAtK = results.subList(0, Math.min(results.size(), k));
		long tp = resultsAtK.stream().filter(r -> r.rel > RELEVANCE_THRESHOLD).count();
		return ((double) tp) / k;
	}

	protected static double mrr(List<QueryResult> results) {
		for (int i = 0; i < results.size(); i++) {
			if (results.get(i).rel > RELEVANCE_THRESHOLD) {
				return (1.0 / (i + 1));
			}
		}
		return 0;
	}

	protected static double averagePrecision(List<QueryResult> results, double rels) {
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

	protected static double ndcg(final List<QueryResult> results, int p) {
		double dcg = 0;
		for (int i = 1; i <= Math.min(p, results.size()); i++) {
			if (results.get(i - 1).rel > RELEVANCE_THRESHOLD) {
				dcg += results.get(i - 1).rel / (Math.log(i + 1) / Math.log(2));
			}
		}
		double idcg = idcg(results);
		if (idcg == 0)
			return 0;
		if (dcg < 0 || idcg < 0) {
			System.out.println("negative ndcg dcg: " + dcg + " idcg: " + idcg);
		}
		return dcg / idcg;
	}

	protected static double idcg(final List<QueryResult> results) {
		List<QueryResult> rels = results.stream().filter(r -> r.rel > RELEVANCE_THRESHOLD).collect(Collectors.toList());
		double idcg = 0;
		for (int i = 1; i <= rels.size(); i++) {
			idcg += (Math.pow(2, results.get(i - 1).rel) - 1) / (Math.log(i + 1) / Math.log(2));
		}
		return idcg;
	}

	protected static List<Integer> buildThresholdsList(List<Integer> clickCounts, int partitionCount) {
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

	protected static class QueryResult {
		double bm25;
		int rel;
		URL url;

		public QueryResult(double bm25, int rel, int clickCount) {
			this.bm25 = bm25;
			this.rel = rel;
			this.url = new URL("NULL", clickCount);
		}

		public QueryResult(double bm25, int rel, URL urlClickCount) {
			this.bm25 = bm25;
			this.rel = rel;
			this.url = urlClickCount;
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

	protected static class URL {
		String urlId;
		int clickCount;

		public URL(String urlId, int clickCount) {
			this.urlId = urlId;
			this.clickCount = clickCount;
		}

		@Override
		public boolean equals(Object obj) {
			URL ucc = (URL) obj;
			return urlId.equals(ucc.urlId);
		}
	}
}
