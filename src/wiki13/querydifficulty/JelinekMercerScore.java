package wiki13.querydifficulty;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import query.ExperimentQuery;

public class JelinekMercerScore implements QueryDifficultyScoreInterface {

    private static final Logger LOGGER = Logger
	    .getLogger(JelinekMercerScore.class.getName());

    private IndexReader globalIndexReader;
    
    public JelinekMercerScore(IndexReader globalIndexReader) {
	this.globalIndexReader = globalIndexReader;
    }
    
    @Override
    public Map<String, Double> computeScore(IndexReader reader,
	    List<ExperimentQuery> queries, String field) throws IOException {
	Map<String, Double> difficulties = new HashMap<String, Double>();
	long tfSum = reader.getSumDocFreq(field);
	long globalTfSum = globalIndexReader.getSumDocFreq(field);
	LOGGER.log(Level.INFO, "Total number of terms in " + field + ": "
		+ tfSum);
	for (ExperimentQuery query : queries) {
	    try (Analyzer analyzer = new StandardAnalyzer()) {
		TokenStream tokenStream = analyzer.tokenStream(field,
			new StringReader(query.getText()));
		CharTermAttribute termAtt = tokenStream
			.addAttribute(CharTermAttribute.class);
		double p = 1.0;
		try {
		    tokenStream.reset();
		    while (tokenStream.incrementToken()) {
			String term = termAtt.toString();
			Term currentTokenTerm = new Term(field, term);
			long tf = reader.totalTermFreq(currentTokenTerm);
			long gtf = globalIndexReader.totalTermFreq(currentTokenTerm);
			double probabilityOfTermGivenSubset = tf / Math.max(tfSum, 1);
			double probabilityOfTermGivenDatabase = gtf / Math.max(globalTfSum, 1); 
			p *= 0.5 * probabilityOfTermGivenSubset + 0.5 * probabilityOfTermGivenDatabase;
		    }
		    tokenStream.end();
		} finally {
		    tokenStream.close();
		}
		difficulties.put(query.getText(), p);
	    }
	}
	return difficulties;
    }

}
