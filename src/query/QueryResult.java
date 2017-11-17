package query;

import indexing.InexFile;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryResult {

	private static final Logger LOGGER = Logger.getLogger(QueryResult.class
			.getName());

	public ExperimentQuery query;
	protected List<String> topResults = new ArrayList<String>();
	protected List<String> topResultsTitle = new ArrayList<String>();
	protected List<String> explanations = new ArrayList<String>();

	public QueryResult(ExperimentQuery query) {
		this.query = query;
	}

	public void addResult(String docId, String docTitle) {
		topResults.add(docId);
		topResultsTitle.add(docId + ": " + docTitle);
	}

	public void addResult(String docId, String docTitle, String explanation) {
		topResults.add(docId);
		topResultsTitle.add(docId + ": " + docTitle);
		explanations.add(explanation);
	}

	public double averagePrecision() {
		double pk = precisionAtK(1);
		double ap = pk;
		for (int k = 1; k < topResults.size(); k++) {
			if (query.hasQrelId(topResults.get(k)))
				ap += (pk * k + 1) / (k + 1);
			pk = (pk * k) / (k + 1);
		}
		return ap / query.getQrelScoreMap().size();
	}

	public double idcg(int p) {
		List<Integer> qrelScoreList = new ArrayList<Integer>();
		if (query.getQrelScoreMap().size() > 0)
			qrelScoreList.addAll(query.getQrelScoreMap().values());
		Collections.sort(qrelScoreList, Collections.reverseOrder());
		double dcg = 0;
		for (int i = 0; i < Math.min(qrelScoreList.size(), p); i++) {
			dcg += qrelScoreList.get(i) / (Math.log(i + 2) / Math.log(2));
		}
		return dcg;
	}

	public double mrr() {
		for (int i = 0; i < topResults.size(); i++) {
			if (query.hasQrelId(topResults.get(i))) {
				return (1.0 / (i + 1));
			}
		}
		return 0;
	}

	public double ndcg(int p) {
		double dcg = 0;
		for (int i = 0; i < Math.min(p, topResults.size()); i++) {
			String currentResult = topResults.get(i);
			if (query.getQrelScoreMap().containsKey(currentResult)) {
				dcg += query.getQrelScoreMap().get(currentResult)
						/ (Math.log(i + 2) / Math.log(2));
			}
		}
		double idcg = idcg(p);
		if (idcg == 0)
			return 0;
		return dcg / idcg;
	}

	public double precisionAtK(int k) {
		double truePositives = 0;
		for (int i = 0; i < Math.min(k, topResults.size()); i++) {
			if (query.getQrelScoreMap().containsKey(topResults.get(i)))
				truePositives++;
		}
		return truePositives / k;
	}

	public double recallAtK(int k) {
		double truePositives = 0;
		for (int i = 0; i < Math.min(k, topResults.size()); i++) {
			if (query.getQrelScoreMap().containsKey(topResults.get(i)))
				truePositives++;
		}
		return truePositives / query.getQrelScoreMap().size();
	}

	public String listFalseNegatives(int k) {
		StringBuilder sb = new StringBuilder();
		int counter = 0;
		for (String qrel : query.getQrelScoreMap().keySet()) {
			if (!topResults.contains(qrel)) {
				sb.append(query.getId() + "," + query.getText() + "," + qrel
						+ "\n");
			}
			if (++counter > k)
				break;
		}
		return sb.toString();
	}

	public String miniLog(Map<String, Double> idPopMap, int k) {
		StringBuilder sb = new StringBuilder();
		sb.append(this.query.getText() + "\n");
		int counter = 1;
		Set<String> rels = this.query.getQrelScoreMap().keySet();
		for (int i = 0; i < this.getTopResults().size(); i++) {
			if (counter++ > k)
				break;
			String docId = this.getTopResults().get(i);
			String rel1Char = "-";
			if (rels.contains(docId)) {
				rel1Char = "+";
			}
			String explanation = "";
			if (explanations.get(i) != null) {
				explanation = explanations.get(i).toString();
			}
			sb.append("\t" + rel1Char + "\t" + idPopMap.get(docId) + "\t"
					+ this.getTopResultsTitle().get(i) + "\t" + explanation.replace("\n", " ") + "\n");

		}
		sb.append("...");
		int i = Math.min(1000, this.getTopResults().size());
		String docId = this.getTopResults().get(i);
		String rel1Char = "-";
		if (rels.contains(docId)) {
			rel1Char = "+";
		}
		String explanation = "";
		if (explanations.get(i) != null) {
			explanation = explanations.get(i).toString();
		}
		sb.append("\t" + rel1Char + "\t" + idPopMap.get(docId) + "\t"
				+ this.getTopResultsTitle().get(i) + "\t" + explanation.replace("\n", " ") + "\n");
		sb.append("======================\n");
		return sb.toString();
	}

	@Deprecated
	public String logForImdb(Map<String, InexFile> idToInexfile) {
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + query.getId() + "\t" + query.getText() + "\n");
		sb.append("|relevant tuples| = " + query.getQrelScoreMap().size()
				+ "\n");
		sb.append("|returned results| = " + this.topResults.size() + "\n");
		int counter = 0;
		sb.append("available missed files: \n");
		for (String rel : query.getQrelScoreMap().keySet()) {
			if (!topResults.contains(rel) && idToInexfile.containsKey(rel)) {
				sb.append(rel + "\t" + idToInexfile.get(rel).title + "\n");
			}
			if (++counter > 20)
				break;
		}
		sb.append("unavailable missed files: \n");
		counter = 0;
		for (String rel : query.getQrelScoreMap().keySet()) {
			if (!topResults.contains(rel) && !idToInexfile.containsKey(rel)) {
				sb.append(rel + "\n");
			}
			if (++counter > 20)
				break;
		}
		sb.append("top 20: \n");
		counter = 0;
		for (String topResult : topResultsTitle) {
			sb.append(topResult + "\t");
			if (++counter > 20)
				break;
		}
		sb.append("top false positives: \n");
		counter = 0;
		for (int i = 0; i < this.topResults.size(); i++) {
			if (!query.hasQrelId(topResults.get(i)))
				sb.append(topResultsTitle.get(i) + "\n");
			if (++counter > 20)
				break;
		}
		sb.append("-------------------------------------\n");
		return sb.toString();
	}

	public String resultString() {
		return query.getText() + "," + precisionAtK(10) + ","
				+ precisionAtK(20) + "," + mrr() + "," + averagePrecision()
				+ "," + recallAtK(200) + "," + recallAtK(1000) + "," + ndcg(10);
	}

	public List<String> getTopResults() {
		return topResults;
	}

	public void setTopResults(List<String> topResults) {
		this.topResults = topResults;
	}

	public List<String> getTopResultsTitle() {
		return topResultsTitle;
	}

	public void setTopResultsTitle(List<String> topResultsTitle) {
		this.topResultsTitle = topResultsTitle;
	}

	public List<String> getExplanations() {
		return explanations;
	}

	public void setExplanations(List<String> explanations) {
		this.explanations = explanations;
	}

	@Override
	public String toString() {
		return query.getId() + "," + query.getText();
	}

	public static void logResultsWithPopularity(List<QueryResult> results,
			Map<String, Double> idPopMap, String logFilePath, int k) {
		LOGGER.log(Level.INFO, "Logging query results..");
		try (FileWriter fw = new FileWriter(logFilePath)) {
			for (QueryResult result : results) {
				fw.write(result.miniLog(idPopMap, k));
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

}
