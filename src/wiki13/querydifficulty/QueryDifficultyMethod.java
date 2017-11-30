package wiki13.querydifficulty;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexReader;

import query.ExperimentQuery;

public interface QueryDifficultyMethod {

    public List<Double> computeScore(IndexReader reader,
	    List<ExperimentQuery> queries, String field) throws IOException;

}
