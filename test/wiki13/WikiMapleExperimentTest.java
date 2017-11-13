package wiki13;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import query.ExperimentQuery;
import query.QueryResult;

public class WikiMapleExperimentTest {

	@Test
	public void testFilterQueryResult() {
		QueryResult result1 = new QueryResult(new ExperimentQuery(1, "query text"));
		result1.addResult("doc1", "good doc");
		result1.addResult("doc2", "fairDoc");
		result1.addResult("doc3", "bad doc");
		Map<String, Double> idPopMap = new HashMap<String, Double>();
		idPopMap.put("doc1", 1.0);
		idPopMap.put("doc2", 10.0);
		idPopMap.put("doc3", 100.0);
		assertEquals(3, result1.getTopResults().size());
		WikiMapleExperiment.filterQueryResult(result1, idPopMap, 1);
		assertEquals(3, result1.getTopResults().size());
		WikiMapleExperiment.filterQueryResult(result1, idPopMap, 0.5);
		assertEquals(1, result1.getTopResults().size());
	}

}
