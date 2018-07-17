package irstyle;

import query.ExperimentQuery;

class QueryResult {
	ExperimentQuery query;
	double rrank = 0;
	long execTime = 0;

	public QueryResult(ExperimentQuery query, double rrank, long execTime) {
		this.query = query;
		this.rrank = rrank;
		this.execTime = execTime;
	}
}