package wiki13.cache_selection;

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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;

import query.ExperimentQuery;

public class BigramJelinekMercerScore implements QueryDifficultyScoreInterface {

	private static final Logger LOGGER = Logger.getLogger(BigramJelinekMercerScore.class.getName());

	private IndexReader globalIndexReader;

	public BigramJelinekMercerScore(IndexReader globalIndexReader) {
		this.globalIndexReader = globalIndexReader;
	}

	@Override
	public Map<String, Double> computeScore(IndexReader reader, List<ExperimentQuery> queries, String field)
			throws IOException {
		Map<String, Double> difficulties = new HashMap<String, Double>();
		long tfSum = reader.getSumTotalTermFreq(field);
		long globalTfSum = globalIndexReader.getSumTotalTermFreq(field);
		LOGGER.log(Level.INFO, "TF sum:" + tfSum);
		LOGGER.log(Level.INFO, "Global TF sum:" + globalTfSum);
		for (ExperimentQuery query : queries) {
			LOGGER.log(Level.INFO, query.getText());
			try (Analyzer analyzer = new StandardAnalyzer()) {
				TokenStream tokenStream = analyzer.tokenStream(field,
						new StringReader(query.getText().replaceAll("'", "`")));
				CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
				double p = 1.0;
				IndexSearcher subsetSearcher = new IndexSearcher(reader);
				try {
					tokenStream.reset();
					String prevTerm = null;
					double prevTf = 0;
					while (tokenStream.incrementToken()) {
						String term = termAtt.toString();
						Term currentTokenTerm = new Term(field, term);
						double tf = reader.totalTermFreq(currentTokenTerm);
						double gtf = globalIndexReader.totalTermFreq(currentTokenTerm);
						if (gtf == 0) {
							LOGGER.log(Level.WARNING, "zero gtf for: " + term);
						}
						double prTermGivenSubset = tf / tfSum;
						double prTermGivenDatabase = gtf / globalTfSum;
						LOGGER.log(Level.INFO, "Pr(" + term + "|subset) = " + prTermGivenSubset + " Pr(term|db) = "
								+ prTermGivenDatabase);
						double prCurrentTerm = (0.5 * prTermGivenSubset + 0.5 * prTermGivenDatabase);
						if (prevTerm != null) {
							PhraseQuery.Builder builder = new PhraseQuery.Builder();
							builder.add(new Term(field, prevTerm), 0);
							builder.add(new Term(field, term), 1);
							PhraseQuery pq = builder.build();
							ScoreDoc[] hits = subsetSearcher.search(pq, 10000).scoreDocs;
							double prBigramGivenPrev = hits.length / prevTf;
							double prBigram = 0.9 * prBigramGivenPrev + 0.1 * prCurrentTerm;
							p *= prBigram;
							LOGGER.log(Level.INFO, "bigram freq = " + hits.length + " prev-tf = " + prevTf);
							LOGGER.log(Level.INFO,
									"Pr(" + prevTerm + " " + term + " | " + prevTerm + " ) = " + prBigram);
						}
						// p *= prCurrent;
						prevTerm = term;
						prevTf = tf;
					}
					tokenStream.end();
				} finally {
					tokenStream.close();
				}
				LOGGER.log(Level.INFO, "p = " + p);
				difficulties.put(query.getText(), p);
			}
		}
		return difficulties;
	}
}
