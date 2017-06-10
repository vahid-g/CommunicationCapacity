package query;

import indexing.InexFile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryResult {

	public ExperimentQuery query;
	public List<String> topResults = new ArrayList<String>();
	public List<String> topResultsTitle = new ArrayList<String>();

	public QueryResult(ExperimentQuery query) {
		this.query = query;
	}

	public double precisionAtK(int k) {
		double count = 0;
		for (int i = 0; i < Math.min(k, topResults.size()); i++) {
			if (query.qrels.contains(topResults.get(i))) {
				count++;
			}
		}
		return count / k;
	}

	public double recallAtK(int k) {
		double count = 0;
		for (int i = 0; i < Math.min(k, topResults.size()); i++) {
			if (query.qrels.contains(topResults.get(i))) {
				count++;
			}
		}
		return count / query.qrels.size();
	}

	public double mrr() {
		for (int i = 0; i < topResults.size(); i++) {
			if (query.qrels.contains(topResults.get(i)))
				return (1.0 / (i + 1));
		}
		return 0;
	}

	public double averagePrecision() {
		double pk = precisionAtK(1);
		double ap = pk;
		for (int k = 1; k < topResults.size(); k++) {
			if (query.qrels.contains(topResults.get(k)))
				ap += (pk * k + 1) / (k + 1);
			pk = (pk * k) / (k + 1);
		}
		return ap / query.qrels.size();
	}

	@Override
	public String toString() {
		return query.getText() + "," + precisionAtK(10) + "," + mrr() + ", " + averagePrecision();
	}

	public String resultString() {
		return query.getText() + "," + precisionAtK(10) + "," + precisionAtK(20) + "," + mrr() + ","
				+ averagePrecision() + "," + recallAtK(200) + "," + recallAtK(1000);
	}

	public String logTopResults() {
		int k = 20;
		int limit = topResultsTitle.size() > k ? k : topResultsTitle.size();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < limit - 1; i++) {
			sb.append(topResultsTitle.get(i) + ",");
		}
		if (limit > 0)
			sb.append(topResultsTitle.get(limit - 1));
		String resultTuples = sb.toString();
		return query.getText() + "," + resultTuples;
	}

	public String listFalseNegatives(int k) {
		StringBuilder sb = new StringBuilder();
		int counter = 0;
		for (String rel : this.query.qrels) {
			if (!topResults.contains(rel)) {
				sb.append(query.id + "," + query.getText() + "," + rel + "\n");
			}
			if (++counter > k)
				break;
		}
		return sb.toString();
	}

	public String miniLog(Map<String, InexFile> idToInexfile) {
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + this.query.id + "\t" + query.getText() + "\n");
		sb.append("|relevant tuples| = " + this.query.qrels.size() + "\n");
		sb.append("|returned results| = " + this.topResults.size() + "\n");
		int counter = 0;
		sb.append("available missed files: \n");
		for (String rel : this.query.qrels) {
			if (!topResults.contains(rel) && idToInexfile.containsKey(rel)) {
				sb.append(rel + "\t" + idToInexfile.get(rel).title + "\n");
			}
			if (++counter > 20)
				break;
		}
		sb.append("unavailable missed files: \n");
		counter = 0;
		for (String rel : this.query.qrels) {
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
			if (!this.query.qrels.contains(topResults.get(i)))
				sb.append(topResultsTitle.get(i) + "\n");
			if (++counter > 20)
				break;
		}
		sb.append("-------------------------------------\n");
		return sb.toString();
	}
}
