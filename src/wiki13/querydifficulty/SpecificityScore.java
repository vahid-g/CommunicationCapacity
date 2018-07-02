package wiki13.querydifficulty;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import query.ExperimentQuery;

import java.io.IOException;
import java.io.StringReader;
import static java.lang.Math.log;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SpecificityScore implements QueryDifficultyScoreInterface {

    private static final Logger LOGGER = Logger.getLogger(SpecificityScore.class.getName());

    @Override
    public Map<String, Double> computeScore(IndexReader reader, List<ExperimentQuery> queries, String field) throws IOException {
        Map<String, Double> difficulties = new HashMap<String, Double>();
        long titleTermCount = reader.getSumTotalTermFreq(field);
        LOGGER.log(Level.INFO, "Total number of terms in " + field + ": " + titleTermCount);
        for (ExperimentQuery query : queries) {
            try (Analyzer analyzer = new StandardAnalyzer()){
                difficulties.put(query.getText(), computeScore(reader, query.getText(), field, analyzer));
            }
        }
        return difficulties;
    }

    public static double computeScore(IndexReader indexReader, String query, String field, Analyzer analyzer) throws IOException {
        double spec = 0;

        long tfSum = indexReader.getSumTotalTermFreq(field);
        HashMap<String, Integer> tokensTextCount = new HashMap<>();
        double tfQSum = 0;
        try {
            TokenStream tokenStream = analyzer.tokenStream(field,
                    new StringReader(query.replaceAll("'", "`")));
            CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                String term = termAtt.toString();
                tokensTextCount.put(term, 1 + tokensTextCount.getOrDefault(term, 0));
                tfQSum += 1;
            }
            tokenStream.close();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        for(String term : tokensTextCount.keySet()) {
            double tfQ = tokensTextCount.get(term);
            Term currentTokenTerm = new Term(field, term);
            double tf = indexReader.totalTermFreq(currentTokenTerm);
            double probabilityOfTermGivenSubset = (tf+tfQ) / (tfSum+tfQSum);
            double probabilityOfTermGivenQuery = tokensTextCount.get(term) / tfQSum;
            spec += probabilityOfTermGivenQuery * log(probabilityOfTermGivenQuery / probabilityOfTermGivenSubset);
        }
        return spec;
    }
}
