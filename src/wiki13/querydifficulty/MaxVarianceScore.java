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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;

import query.ExperimentQuery;

public class MaxVarianceScore implements QueryDifficultyScoreInterface {

    private static final Logger LOGGER = Logger
	    .getLogger(MaxVarianceScore.class.getName());

    @Override
    public Map<String, Double> computeScore(IndexReader reader,
	    List<ExperimentQuery> queries, String field) throws IOException {
	Map<String, Double> difficulties = new HashMap<String, Double>();
	long titleTermCount = reader.getSumTotalTermFreq(field);
	LOGGER.log(Level.INFO, "Total number of terms in " + field + ": "
		+ titleTermCount);
	IndexSearcher searcher = new IndexSearcher(reader);
	searcher.setSimilarity(new BM25Similarity());
	for (ExperimentQuery query : queries) {
	    List<String> terms = Arrays
		    .asList(query.getText().split("[ \"'+]")).stream()
		    .filter(str -> !str.isEmpty()).collect(Collectors.toList());
	    double maxVar = 0;
	    for (String term : terms) {
		Query termQuery = new TermQuery(new Term(field, term));
		TopDocs topDocs = searcher.search(termQuery, 12000000);
		LOGGER.log(Level.INFO, "term: " + term + " #retrieved docs: "
			+ topDocs.totalHits);
		double scoreSum = 0;
		double scoreSquareSum = 0;
		for (int i = 0; i < topDocs.totalHits; i++) {
		    double score = topDocs.scoreDocs[i].score;
		    scoreSum += score;
		    scoreSquareSum += Math.pow(score, 2);
		}
		double ex = scoreSum / topDocs.totalHits;
		if (topDocs.totalHits > 0) {
		    double var = (scoreSquareSum / topDocs.totalHits)
			    - Math.pow(ex, 2);
		    maxVar = Math.max(var, maxVar);
		}

	    }
	    difficulties.put(query.getText(), maxVar);
	}
	return difficulties;
    }
}
