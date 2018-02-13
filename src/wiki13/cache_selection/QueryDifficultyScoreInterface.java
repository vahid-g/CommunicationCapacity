package wiki13.cache_selection;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;

import query.ExperimentQuery;

public interface QueryDifficultyScoreInterface {

    public Map<String, Double> computeScore(IndexReader reader,
	    List<ExperimentQuery> queries, String field) throws IOException;

}
