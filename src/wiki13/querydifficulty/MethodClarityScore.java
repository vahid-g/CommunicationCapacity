package wiki13.querydifficulty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import query.ExperimentQuery;

public class MethodClarityScore implements QueryDifficultyMethod {

    private static final Logger LOGGER = Logger
	    .getLogger(QueryDifficultyMethod.class.getName());

    @Override
    public List<Double> computeScore(IndexReader reader,
	    List<ExperimentQuery> queries, String field) throws IOException {
	List<Double> difficulties = new ArrayList<Double>();
	long titleTermCount = reader.getSumTotalTermFreq(field);
	LOGGER.log(Level.INFO, "Total number of terms in " + field + ": "
		+ titleTermCount);
	for (ExperimentQuery query : queries) {
	    List<String> terms = Arrays
		    .asList(query.getText().split("[ \"'+]")).stream()
		    .filter(str -> !str.isEmpty()).collect(Collectors.toList());
	    int qLength = terms.size();
	    long termCountSum = 0;
	    for (String term : terms) {
		termCountSum += reader.totalTermFreq(new Term(field, term));
	    }
	    double ictf = Math.log(titleTermCount / (termCountSum + 1.0));
	    difficulties.add(1.0 / qLength + ictf / qLength);
	}
	return difficulties;
    }

}
