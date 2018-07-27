package irstyle;

import java.util.ArrayList;
import java.util.List;

import irstyle_core.Result;
import query.ExperimentQuery;

class IRStyleQueryResult {
	ExperimentQuery query;
	long execTime = 0;
	List<String> resultIDs = new ArrayList<String>();

	public IRStyleQueryResult(ExperimentQuery query, long execTime) {
		this.query = query;
		this.execTime = execTime;
	}

	void addIRStyleResults(ArrayList<Result> results) {
		for (Result result : results) {
			String resultText = result.getStr();
			resultIDs.add(resultText.substring(0, resultText.indexOf(" - ")));
		}
	}

	double rrank() {
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
		return relCount / resultIDs.size();
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

}