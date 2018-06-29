package data_processing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MsnQueryLogMiner {

	public static class MsnLogQuery {
		String text = "";
		String id = "";
		List<String> relWikiId = new ArrayList<String>();
		int freq = 0;
		String type = "";
	}

	final static String DELIMITER = "\t";

	public static void main(String[] args) throws IOException {
		String queryFreqPath = "data/wiki/msn_gen/query_freq.csv";
		String queryQidPath = "data/wiki/msn_origin/query_qid.csv";
		String qidQueryFreqPath = "data/wiki/msn_gen/qid_query_freq.csv";
		String qidQrelPath = "data/wiki/msn_origin/msn.qrels";
		String qidQueryFreqQrelPath = "data/wiki/msn_gen/qid_query_freq_qrel.csv";
		mineQueryFrequencies("data/wiki/msn_origin/msn_queries.txt", queryFreqPath, true);
		generateQidQueryFreq(queryFreqPath, queryQidPath, qidQueryFreqPath);
		generateQidQueryFreqQrel(qidQrelPath, qidQueryFreqPath, qidQueryFreqQrelPath);
		generateQrelCounts("/scratch/data-sets/wikipedia/textpath13_count13_title.csv", qidQueryFreqQrelPath,
				"data/wiki/msn_gen/qid_query_freq_qrel_count.csv");

	}

	static List<MsnLogQuery> mineQueryFrequencies(String dataset, String output, boolean writeToFile) {
		List<MsnLogQuery> queries = new ArrayList<MsnLogQuery>();
		try (BufferedReader br = new BufferedReader((new FileReader(dataset)))) {
			String line = br.readLine();
			Pattern firstLinePtr = Pattern.compile("# (\\d+) -+");
			Pattern wikiPtr = Pattern.compile("# wikiID : (.+)");
			Pattern freqPtr = Pattern.compile("# frequency : (\\d+)");
			Pattern entityPtr = Pattern.compile("# entityID : .*");
			MsnLogQuery query = null;
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
					query.relWikiId.add(wikiMatcher.group(1));
				} else if (freqMatcher.find()) {
					query.freq += Integer.parseInt(freqMatcher.group(1).trim());
				} else if (entityMatcher.find()) {
					query.text = br.readLine();
				}
				line = br.readLine();
			}
			queries.add(query);
			if (writeToFile) {
				try (FileWriter fw = new FileWriter(output)) {
					for (MsnLogQuery mlq : queries) {
						fw.write(mlq.text + DELIMITER + mlq.freq + "\n");
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return queries;
	}

	static void generateQidQueryFreq(List<MsnLogQuery> queries, String queryQidPath) throws IOException {
		Map<String, String> queryQid = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(queryQidPath))) {
			String line = "";
			while ((line = br.readLine()) != null) {
				int i = line.lastIndexOf(' ');
				if (i == -1) {
					System.err.println("error processing line: " + line);
				} else {
					String query = line.substring(0, i);
					String qid = line.substring(i + 1);
					queryQid.put(query, qid);
				}
			}
		}
		for (MsnLogQuery msnQuery : queries) {
			String query = msnQuery.text;
			if (query.equals(",dookie")) {
				query = "dookie";
			} else if (query.equals("rihanna born:")) {
				query = "rihanna born";
			}
			String qid = queryQid.get(query);
			if (qid == null) {
				// MSN querylog contains some nonsense slashes. Next line takes care of them
				qid = queryQid.get(query.replaceAll(" / ", " ").replaceAll(" /", " ").replaceAll("/ ", " ")
						.replaceAll("/", " ").trim());
				if (qid == null) {
					System.err.println("queryQidMap doesn't contain query:" + query);
					continue;
				}
			}
			msnQuery.id = qid;
		}
	}

	static void generateQidQueryFreq(String queryFreqPath, String queryQidPath, String output) throws IOException {
		Map<String, String> queryQid = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(queryQidPath))) {
			String line = "";
			while ((line = br.readLine()) != null) {
				int i = line.lastIndexOf(' ');
				if (i == -1) {
					System.err.println("error processing line: " + line);
				} else {
					String query = line.substring(0, i);
					String qid = line.substring(i + 1);
					queryQid.put(query, qid);
				}
			}
		}
		try (BufferedReader br = new BufferedReader(new FileReader(queryFreqPath));
				FileWriter fw = new FileWriter(output)) {
			String line = "";
			while ((line = br.readLine()) != null) {
				int i = line.lastIndexOf(DELIMITER);
				if (i == -1) {
					System.err.println("line doesn't contain DELIMITER character: " + line);
				} else {
					String query = line.substring(0, i); //.replaceAll("^\"|\"$", "");
					// Next lines take care of specific queries
					if (query.equals(",dookie")) {
						query = "dookie";
					} else if (query.equals("rihanna born:")) {
						query = "rihanna born";
					}
					String qid = queryQid.get(query);
					if (qid == null) {
						// MSN querylog contains some nonsense slashes. Next line takes care of them
						qid = queryQid.get(query.replaceAll(" / ", " ").replaceAll(" /", " ").replaceAll("/ ", " ")
								.replaceAll("/", " ").trim());
						if (qid == null) {
							System.err.println("queryQidMap doesn't contain query:" + query);
							continue;
						}
					}
					fw.write(qid + DELIMITER + line + "\n");
				}
			}
		}
	}

	static void generateQidQueryFreqQrel(String qidQrelPath, String qidQueryFreqPath, String output)
			throws FileNotFoundException, IOException {
		Map<String, String> qidQrelMap = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(qidQrelPath))) {
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] fields = line.split(" ");
				qidQrelMap.put(fields[0], fields[2]);
			}
		}
		try (BufferedReader br = new BufferedReader(new FileReader(qidQueryFreqPath));
				FileWriter fw = new FileWriter(output)) {
			String line = "";
			while ((line = br.readLine()) != null) {
				String qid = line.substring(0, line.indexOf(DELIMITER));
				if (!qidQrelMap.containsKey(qid)) {
					System.err.println("couldn't find qid: " + qid + " in qrels");
				} else {
					fw.write(line + DELIMITER + qidQrelMap.get(qid) + "\n");
				}
			}
		}
	}

	static void generateQrelCounts(String idCountsPath, String qidQueryFreqQrel, String output)
			throws IOException, IOException {
		Map<String, String> qrelCountMap = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(idCountsPath));
				FileWriter fw = new FileWriter(output)) {
			String line = null;
			while ((line = br.readLine()) != null) {
				Pattern pattern = Pattern.compile(".*/([^.]+)\\.txt,([^,]+), (.+)");
				Matcher matcher = pattern.matcher(line);
				if (matcher.find()) {
					String id = matcher.group(1);
					String count = matcher.group(2);
					String title = matcher.group(3);
					qrelCountMap.put(id, count + DELIMITER + title);
				} else {
					System.err.println("Couldn't parse line: " + line);
				}
			}
		}
	
		try (BufferedReader br = new BufferedReader(new FileReader(qidQueryFreqQrel));
				FileWriter fw = new FileWriter(output)) {
			String line = null;
			int failureCounts = 0;
			while ((line = br.readLine()) != null) {
				String qrel = line.substring(line.lastIndexOf(DELIMITER) + 1);
				String countAndTitle = qrelCountMap.get(qrel);
				if (countAndTitle == null) {
					System.err.println("Couldn't find qrel: " + qrel + " in qrelCountsMap");
					failureCounts++;
					countAndTitle = "0, NULL";
				}
				fw.write(line + DELIMITER + countAndTitle + "\n");
			}
			System.err.println("couldn't find " + failureCounts + " qrels in qrelCountsMap");
		}
	}

}
