package inex09;

import java.util.ArrayList;
import java.util.List;

public class InexQueryResult {

	public InexQuery query;
	public List<String> topResults = new ArrayList<String>();

	public InexQueryResult(InexQuery query) {
		this.query = query;
	}

	public double precisionAtK(int k) {
		double count = 0;
		for (int i = 0; i < Math.min(k, topResults.size()); i++) {
			if (query.relDocs.contains(topResults.get(i))) {
				count++;
			}
		}
		return count / k;
	}

	double mrr() {
		for (int i = 0; i < topResults.size(); i++) {
			if (query.relDocs.contains(topResults.get(i)))
				return (1.0 / (i + 1));
		}
		return 0;
	}

	@Override
	public String toString() {
		return query.text + ", " + precisionAtK(3) + ", " + precisionAtK(10)
				+ ", " + precisionAtK(20) + ", " + mrr() + "\n";
	}

	public String top10() {
		int limit = topResults.size() > 10 ? 10 : topResults.size();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < limit - 1; i++){
			sb.append(topResults.get(i) + ",");
		}
		sb.append(topResults.get(limit - 1));
		String resultTuples = sb.toString();
		return query.text + "," + resultTuples;
	}
}
