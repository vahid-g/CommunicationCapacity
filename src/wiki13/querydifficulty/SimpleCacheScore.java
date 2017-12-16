package wiki13.querydifficulty;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import query.ExperimentQuery;

public class SimpleCacheScore implements QueryDifficultyScoreInterface {

    @Override
    public Map<String, Double> computeScore(IndexReader reader, List<ExperimentQuery> queries, String field)
	    throws IOException {
	Map<String, Double> difficulties = new HashMap<String, Double>();
	for (ExperimentQuery query : queries) {
	    double termCounter = 0.0;
	    List<String> terms = Arrays.asList(query.getText().split("[ \"'+]")).stream().filter(str -> !str.isEmpty())
		    .collect(Collectors.toList());
	    for (String term : terms) {
		if (reader.totalTermFreq(new Term(field, term)) > 0) {
		    termCounter++;
		}
	    }
	    difficulties.put(query.getText(), termCounter / terms.size());
	}
	return difficulties;
    }

}
