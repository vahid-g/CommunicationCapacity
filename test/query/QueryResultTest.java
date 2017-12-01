package query;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class QueryResultTest {

    private static final double epsilon = 0.001;

    QueryResult queryResult;

    @Before
    public void init() {
	Set<Qrel> qrels = new HashSet<Qrel>();
	ExperimentQuery eq = new ExperimentQuery(1001, "keyword query text",
		qrels);
	queryResult = new QueryResult(eq);
    }

    @Test
    public void testNdcg() {
	Map<String, Integer> qrelScoreMap = queryResult.query.getQrelScoreMap();
	assertEquals(queryResult.idcg(10), 0, epsilon);
	assertEquals(queryResult.ndcg(10), 0, epsilon);
	qrelScoreMap.put("qrel1", 10);
	assertEquals(queryResult.idcg(10), 10, epsilon);
	assertEquals(queryResult.ndcg(10), 0, epsilon);
	queryResult.addResult("qrel2", "title2");
	assertEquals(queryResult.idcg(10), 10, epsilon);
	assertEquals(queryResult.ndcg(10), 0, epsilon);
	queryResult.addResult("qrel1", "title1");
	assertEquals(queryResult.idcg(10), 10, epsilon);
	assertEquals(queryResult.ndcg(10), 1 / (Math.log(3) / Math.log(2)),
		epsilon);
    }

    @Test
    public void testPrecisionAtK() {
	Map<String, Integer> qrelScoreMap = queryResult.query.getQrelScoreMap();
	queryResult.addResult("q2", "t1");
	assertEquals(queryResult.precisionAtK(10), 0, epsilon);
	qrelScoreMap.put("q1", 10);
	qrelScoreMap.put("q2", 1);
	qrelScoreMap.put("q3", 2);
	assertEquals(queryResult.precisionAtK(10), 0.1, epsilon);
	queryResult.addResult("q3", "t1");
	assertEquals(queryResult.precisionAtK(10), 0.2, epsilon);
	queryResult.addResult("q4", "t1");
	for (int i = 0; i < 7; i++) {
	    qrelScoreMap.put(i + "", i + 1);
	    queryResult.addResult(i + "", "title");
	}
	assertEquals(queryResult.precisionAtK(10), 0.9, epsilon);
    }
}
