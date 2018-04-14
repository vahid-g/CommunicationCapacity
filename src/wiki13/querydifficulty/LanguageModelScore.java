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

public class LanguageModelScore implements QueryDifficultyScoreInterface {

	private static final Logger LOGGER = Logger.getLogger(LanguageModelScore.class.getName());

	@Override
	public Map<String, Double> computeScore(IndexReader reader, List<ExperimentQuery> queries, String field)
			throws IOException {
		Map<String, Double> difficulties = new HashMap<String, Double>();
		long subsetTermCount = reader.getSumDocFreq(field);
		long subsetVocabSize = reader.getSumTotalTermFreq(field);
		LOGGER.log(Level.INFO, "Total number of terms in " + field + ": " + subsetTermCount);
		LOGGER.log(Level.INFO, "Vocab size: " + subsetVocabSize);
		for (ExperimentQuery query : queries) {
			try (Analyzer analyzer = new StandardAnalyzer()) {
				TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.getText()));
				CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
				double p = 1.0;
				try {
					tokenStream.reset();
					while (tokenStream.incrementToken()) {
						String term = termAtt.toString();
						long tf = reader.totalTermFreq(new Term(field, term));
						double probabilityOfTermGivenSubset = (tf + 1.0) / (subsetTermCount + subsetVocabSize);
						p *= probabilityOfTermGivenSubset;
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
