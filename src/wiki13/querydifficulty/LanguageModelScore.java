package wiki13.querydifficulty;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import query.ExperimentQuery;

public class LanguageModelScore implements QueryDifficultyScoreInterface {

    private static final Logger LOGGER = Logger
	    .getLogger(LanguageModelScore.class.getName());

    @Override
    public Map<String, Double> computeScore(IndexReader reader,
	    List<ExperimentQuery> queries, String field) throws IOException {
	Map<String, Double> difficulties = new HashMap<String, Double>();
	long titleTermCount = reader.getSumTotalTermFreq(field);
	LOGGER.log(Level.INFO, "Total number of terms in " + field + ": "
		+ titleTermCount);
	for (ExperimentQuery query : queries) {
	    List<String> terms = Arrays
		    .asList(query.getText().split("[ \"'+]")).stream()
		    .filter(str -> !str.isEmpty()).collect(Collectors.toList());
	    long subsetTermCount = reader.getSumDocFreq(field);
	    long subsetVocabSize = reader.getSumTotalTermFreq(field);
	    double p = 1;
	    for (String term : terms) {
		long tf = reader.totalTermFreq(new Term(field, term));
		double probabilityOfTermGivenSubset = (tf + 1)
			/ (subsetTermCount + subsetVocabSize);
		p *= probabilityOfTermGivenSubset;
	    }
	    difficulties.put(query.getText(), p);
	}
	return difficulties;
    }

}
