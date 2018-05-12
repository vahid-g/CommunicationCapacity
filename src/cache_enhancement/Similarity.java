package cache_enhancement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import query.ExperimentQuery;
import wiki13.querydifficulty.QueryDifficultyScoreInterface;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.log;

public class Similarity implements QueryDifficultyScoreInterface {
    private static final Logger LOGGER = Logger.getLogger(Similarity.class.getName());

    private IndexReader globalIndexReader;

    public Similarity (IndexReader globalIndexReader) {
        this.globalIndexReader = globalIndexReader;
    }

    @Override
    public Map<String, Double> computeScore(IndexReader reader, List<ExperimentQuery> queries, String field) throws IOException {
        return klDivergence(reader, queries, field);
    }

    Map<String, Double> klDivergence(IndexReader reader, List<ExperimentQuery> queries, String field) throws IOException {
        HashMap<String, Double> distance = new HashMap<>();
        long tfSum = reader.getSumTotalTermFreq(field);
//        long globalTfSum = globalIndexReader.getSumTotalTermFreq(field);
        LOGGER.log(Level.INFO, "TF sum:" + tfSum);
//        LOGGER.log(Level.INFO, "Global TF sum:" + globalTfSum);
        for (ExperimentQuery query : queries) {
            LOGGER.log(Level.INFO, query.getText());
            HashMap<String, Integer> tokensTextCount = new HashMap<>();
            double tfQSum = 0;
            try (Analyzer analyzer = new StandardAnalyzer()) {
                TokenStream tokenStream = analyzer.tokenStream(field,
                        new StringReader(query.getText().replaceAll("'", "`")));
                CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
                try {
                    tokenStream.reset();
                    while (tokenStream.incrementToken()) {
                        String term = termAtt.toString();
                        tokensTextCount.put(term, 1 + tokensTextCount.getOrDefault(term, 0));
                        tfQSum += 1;
                    }
                    tokenStream.end();
                } finally {
                    tokenStream.close();
                }
            }

            double kl = 0.0;
            for(String term : tokensTextCount.keySet()) {
                double tfQ = tokensTextCount.get(term);
                Term currentTokenTerm = new Term(field, term);
                double tf = reader.totalTermFreq(currentTokenTerm);
//                        double gtf = globalIndexReader.totalTermFreq(currentTokenTerm);
//                        if (gtf == 0) {
//                            LOGGER.log(Level.WARNING, "zero gtf for: " + term);
//                        }
                double probabilityOfTermGivenSubset = (tf+tfQ) / (tfSum+tfQSum);
//                        double probabilityOfTermGivenDatabase = gtf / globalTfSum;
                double probabilityOfTermGivenQuery = tokensTextCount.get(term) / tfQSum;
                kl += probabilityOfTermGivenQuery * log(probabilityOfTermGivenQuery / probabilityOfTermGivenSubset);
            }
            LOGGER.log(Level.INFO, "KL = " + kl);
            distance.put(query.getText(), kl);
        }
        return distance;
    }
}
