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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.log;

public class SimilarityScore implements QueryDifficultyScoreInterface {

    private static final Logger LOGGER = Logger.getLogger(SimilarityScore.class.getName());

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
        double simi = 0;
        return simi;
    }
}
