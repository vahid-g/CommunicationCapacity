package wiki13.querydifficulty;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import query.ExperimentQuery;

public class SimpleCacheScore implements QueryDifficultyScoreInterface {

	@Override
	public Map<String, Double> computeScore(IndexReader reader, List<ExperimentQuery> queries, String field)
			throws IOException {
		Map<String, Double> difficulties = new HashMap<String, Double>();
		try (Analyzer analyzer = new StandardAnalyzer()) {
			for (ExperimentQuery query : queries) {
				double termCounter = 0.0;
				double allTermsCounter = 0.0;
				TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.getText()));
				CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
				try {
					tokenStream.reset();
					while (tokenStream.incrementToken()) {
						allTermsCounter++;
						String term = termAtt.toString();
						if (reader.totalTermFreq(new Term(field, term)) > 0) {
							termCounter++;
						}
					}
					tokenStream.end();
				} finally {
					tokenStream.close();
				}
				difficulties.put(query.getText(), termCounter / allTermsCounter);
			}
		}
		return difficulties;
	}

}
