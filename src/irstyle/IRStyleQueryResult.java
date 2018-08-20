package irstyle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import irstyle.core.Result;
import query.ExperimentQuery;

public class IRStyleQueryResult {
	ExperimentQuery query;
	public long execTime = 0;
	long tuplesetTime = 0;
	List<String> resultIDs = new ArrayList<String>();

	public IRStyleQueryResult(ExperimentQuery query, long execTime) {
		this.query = query;
		this.execTime = execTime;

	}

	public void addIRStyleResults(ArrayList<Result> results) {
		for (Result result : results) {
			String resultText = result.getStr();
			resultIDs.add(resultText.substring(0, resultText.indexOf(" - ")));
		}
	}

	public double rrank() {
		for (int i = 0; i < resultIDs.size(); i++) {
			String resultId = resultIDs.get(i);
			if (query.getQrelScoreMap().keySet().contains(resultId)) {
				return 1.0 / (i + 1);
			}
		}
		return 0;
	}

	double recall() {
		double relCount = 0.0;
		for (String id : resultIDs) {
			if (query.getQrelScoreMap().keySet().contains(id)) {
				relCount++;
			}
		}
		return relCount / query.getQrelScoreMap().size();
	}

	double recall(Map<ExperimentQuery, Integer> queryRelCountMap) {
		double relCount = 0.0;
		for (String id : resultIDs) {
			if (query.getQrelScoreMap().keySet().contains(id)) {
				relCount++;
			}
		}
		return relCount / queryRelCountMap.get(this.query);
	}

	double p20() {
		double relCount = 0.0;
		for (int i = 0; i < Math.min(20, resultIDs.size()); i++) {
			if (query.getQrelScoreMap().keySet().contains(resultIDs.get(i))) {
				relCount++;
			}
		}
		return relCount / 20.0;
	}

	void dedup() {
		System.out.println("\t size before dedup = " + resultIDs.size());
		this.resultIDs = this.resultIDs.stream().distinct().collect(Collectors.toList());
		System.out.println("\t size after dedup = " + resultIDs.size());
	}

}