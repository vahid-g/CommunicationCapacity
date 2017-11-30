package wiki13.querydifficulty;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import query.ExperimentQuery;

public class QueryDifficultyComputer {

    private static final Logger LOGGER = Logger
	    .getLogger(MethodClarityScoreTest.class.getName());

    private QueryDifficultyMethod method;

    public QueryDifficultyComputer(QueryDifficultyMethod method) {
	this.method = method;
    }

    public List<Double> computeQueryDifficulty(String indexPath,
	    List<ExperimentQuery> queries, String field) {
	List<Double> difficulties = new ArrayList<Double>();
	try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
		.get(indexPath)))) {
	    difficulties = method.computeScore(reader, queries, field);
	} catch (IOException e) {
	    LOGGER.log(Level.SEVERE, e.getMessage(), e);
	}
	return difficulties;
    }

}
