package query;

import java.util.ArrayList;
import java.util.List;

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

	double mrr() {
		for (int i = 0; i < topResults.size(); i++) {
			if (query.qrels.contains(topResults.get(i)))
				return (1.0 / (i + 1));
		}
		return 0;
	}

	@Override
	public String toString() {
		return query.text + ", " + precisionAtK(3) + ", " + precisionAtK(10) + ", " + this.mrr();
	}
	
	public String fullString() {
		return query.text + ", " + precisionAtK(3) + ", " + precisionAtK(10)
				+ ", " + precisionAtK(20) + ", " + mrr() + "," + recallAtK(10)
				+ "," + recallAtK(20) + "," + recallAtK(100) + "," + recallAtK(200);
	}

	public String top10() {
		int limit = topResultsTitle.size() > 10 ? 10 : topResultsTitle.size();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < limit - 1; i++) {
			sb.append(topResultsTitle.get(i) + ",");
		}
		if (limit > 0)
			sb.append(topResultsTitle.get(limit - 1));
		String resultTuples = sb.toString();
		return query.text + "," + resultTuples;
	}
	
	public String listFalseNegatives(int k){
		StringBuilder sb = new StringBuilder();
		int counter = 0;
		for (String rel : this.query.qrels){
			if (!topResults.contains(rel)){
				sb.append(query.id + "," + query.text + "," + rel + "\n");
			}
			if (++counter > k) break;
		}
		return sb.toString();
	}
	
	public String miniLog(){
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + this.query.id + "\t" + query.text + "\n");
		int counter = 0;
		for (String rel : this.query.qrels){
			if (!topResults.contains(rel)){
				sb.append("missed: " + rel + "\n");
			}
			if (++counter > 20) break;
		}
		for (int i = 0; i < this.topResults.size(); i++){
			if (!this.query.qrels.contains(topResults.get(i)))
				sb.append("fp: " + topResults.get(i) + "\t" + topResultsTitle.get(i) + "\n");
		}
		sb.append("-------------------------------------\n");
		return sb.toString();
	}
}
