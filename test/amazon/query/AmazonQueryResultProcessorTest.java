package amazon.query;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import query.ExperimentQuery;
import query.Qrel;
import query.QueryResult;

public class AmazonQueryResultProcessorTest {

	Map<String, String> isbnToLtid = new HashMap<String, String>();
	QueryResult queryResult;

	@Before
	public void init() {
		Set<Qrel> qrelSet = new HashSet<Qrel>();
		qrelSet.add(new Qrel(1, "l1", 1));
		qrelSet.add(new Qrel(1, "l2", 1));
		qrelSet.add(new Qrel(2, "l2", 1));
		qrelSet.add(new Qrel(2, "l3", 1));
		ExperimentQuery eq = new ExperimentQuery(1, "q1", qrelSet);
		queryResult = new QueryResult(eq);
		queryResult.addResult("i1", "t1");
		queryResult.addResult("i2", "t1");
		queryResult.addResult("i3", "t1");
		queryResult.addResult("i4", "t2");
		isbnToLtid.put("i1", "l1");
		isbnToLtid.put("i2", "l1");
		isbnToLtid.put("i3", "l1");
		isbnToLtid.put("i4", "l2");
	}

	@Test
	public void testConvertIsbnAnswersToSetAndFilter() {

		AmazonQueryResultProcessor.convertIsbnAnswersToLtidAndFilter(
				queryResult, isbnToLtid);
		assertEquals(2, queryResult.getTopResults().size());
		assertEquals("l1", queryResult.getTopResults().get(0));
		assertEquals("l2", queryResult.getTopResults().get(1));
	}
	
	

}
