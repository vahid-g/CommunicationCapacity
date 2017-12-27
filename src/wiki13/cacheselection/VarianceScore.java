package wiki13.cacheselection;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;

import query.ExperimentQuery;

public class VarianceScore implements QueryDifficultyScoreInterface {

    public static enum VarianceScoreMode {
	MAX_VARIANCE, AVERAGE_VARIANCE, MAX_EX, AVERAGE_EX
    }

    private static final Logger LOGGER = Logger.getLogger(VarianceScore.class.getName());

    private VarianceScoreMode mode;

    public VarianceScore(VarianceScoreMode mode) {
	super();
	this.mode = mode;
    }

    @Override
    public Map<String, Double> computeScore(IndexReader reader, List<ExperimentQuery> queries, String field)
	    throws IOException {
	Map<String, Double> difficulties = new HashMap<String, Double>();
	long titleTermCount = reader.getSumTotalTermFreq(field);
	LOGGER.log(Level.INFO, "Total number of terms in " + field + ": " + titleTermCount);
	IndexSearcher searcher = new IndexSearcher(reader);
	searcher.setSimilarity(new BM25Similarity());
	for (ExperimentQuery query : queries) {
	    List<String> terms = Arrays.asList(query.getText().split("[ \"'+]")).stream().filter(str -> !str.isEmpty())
		    .collect(Collectors.toList());
	    double maxVar = 0;
	    double maxEx = 0;
	    double varSum = 0;
	    double exSum = 0;
	    QueryParser parser = new QueryParser(field, new StandardAnalyzer());
	    for (String term : terms) {
		try {
		    Query termQuery = parser.parse(term);
		    TopDocs topDocs = searcher.search(termQuery, 12000000);
		    double scoreSum = 0;
		    double scoreSquareSum = 0;
		    for (int i = 0; i < topDocs.totalHits; i++) {
			double score = topDocs.scoreDocs[i].score;
			scoreSum += score;
			scoreSquareSum += Math.pow(score, 2);
		    }
		    double ex = scoreSum / topDocs.totalHits;
		    double var = 0;
		    if (topDocs.totalHits > 0) {
			var = (scoreSquareSum / topDocs.totalHits) - Math.pow(ex, 2);
			maxVar = Math.max(var, maxVar);
			maxEx = Math.max(ex, maxEx);
			varSum += var;
			exSum += ex;
		    }
		    LOGGER.log(Level.INFO, "\t(" + query.getText() + "): E[x] = " + ex + ", Var[x] = " + var + ", N = "
			    + topDocs.totalHits);
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
		    LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	    }
	    if (mode.equals(VarianceScoreMode.MAX_VARIANCE)) {
		difficulties.put(query.getText(), maxVar);
	    } else if (mode.equals(VarianceScoreMode.AVERAGE_VARIANCE)) {
		difficulties.put(query.getText(), varSum / terms.size());
	    } else if (mode.equals(VarianceScoreMode.MAX_EX)) {
		difficulties.put(query.getText(), maxEx);
	    } else if (mode.equals(VarianceScoreMode.AVERAGE_EX)) {
		difficulties.put(query.getText(), exSum / terms.size());
	    } else {
		LOGGER.log(Level.SEVERE, "VarianceScore mode is not defined");
	    }
	}
	return difficulties;
    }
}
