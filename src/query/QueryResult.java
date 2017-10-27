package query;

import indexing.InexFile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class QueryResult {

	public ExperimentQuery query;
	private List<String> topResults = new ArrayList<String>();
	private List<String> topResultsTitle = new ArrayList<String>();

	public QueryResult(ExperimentQuery query) {
		this.query = query;
	}

	public void addResult(String docId, String docTitle) {
		topResults.add(docId);
		topResultsTitle.add(docId + ": " + docTitle);
	}

	public double averagePrecision() {
		double pk = precisionAtK(1);
		double ap = pk;
		for (int k = 1; k < topResults.size(); k++) {
			if (query.hasQrelId(topResults.get(k)))
				ap += (pk * k + 1) / (k + 1);
			pk = (pk * k) / (k + 1);
		}
		return ap / query.getQrels().size();
	}

	public List<String> getTopResults() {
		return topResults;
	}

	public List<String> getTopResultsTitle() {
		return topResultsTitle;
	}
	
	double idcg(int p) {
		List<Qrel> qrelList = new ArrayList<Qrel>();
		qrelList.addAll(query.getQrels());
		Collections.sort(qrelList, new Comparator<Qrel>() {
			@Override
			public int compare(Qrel o1, Qrel o2) {
				if (o1.getRel() > o2.getRel()) {
					return -1;
				} else if (o1.getRel() == o2.getRel()) {
					return 0;
				} else {
					return 1;
				}
			}
		});
		double dcg = 0;
		for (int i = 0; i < Math.min(qrelList.size(), p); i++) {
			dcg += qrelList.get(i).getRel()/(Math.log(i + 2) / Math.log(2));
		}
		return dcg;
	}
	
	public String listFalseNegatives(int k) {
		StringBuilder sb = new StringBuilder();
		int counter = 0;
		for (Qrel qrel : query.getQrels()) {
			String rel = qrel.getQrelId();
			if (!topResults.contains(rel)) {
				sb.append(query.getId() + "," + query.getText() + "," + rel + "\n");
			}
			if (++counter > k)
				break;
		}
		return sb.toString();
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

	public String miniLog(Map<String, InexFile> idToInexfile) {
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + query.getId() + "\t" + query.getText() + "\n");
		sb.append("|relevant tuples| = " + query.getQrels().size() + "\n");
		sb.append("|returned results| = " + this.topResults.size() + "\n");
		int counter = 0;
		sb.append("available missed files: \n");
		for (Qrel qrel : query.getQrels()) {
			String rel = qrel.getQrelId();
			if (!topResults.contains(rel) && idToInexfile.containsKey(rel)) {
				sb.append(rel + "\t" + idToInexfile.get(rel).title + "\n");
			}
			if (++counter > 20)
				break;
		}
		sb.append("unavailable missed files: \n");
		counter = 0;
		for (Qrel qrel : query.getQrels()) {
			String rel = qrel.getQrelId();
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
			for (Qrel qrel : query.getQrels()) {
				if (qrel.getQrelId().equals(topResults.get(i))) {
					dcg += qrel.getRel() / (Math.log(i + 2) / Math.log(2));
					break;
				}
			}
		}
		double idcg = idcg(p);
		if (idcg == 0) return 0;
		return dcg / idcg;
	}

	public double precisionAtK(int k) {
		double truePositives = 0;
		double countK = 0;
		for (Qrel qrel : query.getQrels()) {
			if (countK >= k++)
				break;
			if (topResults.contains(qrel.getQrelId()))
				truePositives++;
		}
		return truePositives / k;
	}

	public double recallAtK(int k) {
		double truePositives = 0;
		double countK = 0;
		for (Qrel qrel : query.getQrels()) {
			if (countK >= k++)
				break;
			if (topResults.contains(qrel.getQrelId()))
				truePositives++;
		}
		return truePositives / query.getQrels().size();
	}

	public String resultString() {
		return query.getText() + "," + precisionAtK(10) + "," + precisionAtK(20) + "," + mrr() + ","
				+ averagePrecision() + "," + recallAtK(200) + "," + recallAtK(1000) + "," + ndcg(10);
	}

	public void setTopResults(List<String> topResults) {
		this.topResults = topResults;
	}

	public void setTopResultsTitle(List<String> topResultsTitle) {
		this.topResultsTitle = topResultsTitle;
	}

	@Override
	public String toString() {
		return query.getText() + "," + precisionAtK(10) + "," + mrr() + ", " + averagePrecision();
	}

}
