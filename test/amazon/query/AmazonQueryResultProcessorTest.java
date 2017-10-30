package amazon.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import query.ExperimentQuery;
import query.Qrel;
import query.QueryResult;
import amazon.popularity.AmazonIsbnPopularityMap;

public class AmazonQueryResultProcessorTest {

	Map<String, String> isbnToLtid = new HashMap<String, String>();
	Map<String, Set<String>> ltidToIsbn = new HashMap<String, Set<String>>();
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
		Set<String> set1 = new HashSet<String>();
		set1.add("i1");
		set1.add("i2");
		set1.add("i3");
		Set<String> set2 = new HashSet<String>();
		set2.add("i4");
		ltidToIsbn.put("l1", set1);
		ltidToIsbn.put("l3", new HashSet<String>());
		ltidToIsbn.put("l4", set2);
	}

	@Test
	public void testConvertIsbnAnswersToSetAndFilter() {
		AmazonQueryResultProcessor.convertIsbnAnswersToLtidAndFilter(
				queryResult, isbnToLtid);
		assertEquals(2, queryResult.getTopResults().size());
		assertEquals("l1", queryResult.getTopResults().get(0));
		assertEquals("l2", queryResult.getTopResults().get(1));
	}
	
	@Test
	public void testGenerateLog() {
		AmazonQueryResultProcessor.convertIsbnAnswersToLtidAndFilter(
				queryResult, isbnToLtid);
		AmazonIsbnPopularityMap aipm = AmazonIsbnPopularityMap
				.getInstance("data/amazon/test_data/path.csv");
		String log = AmazonQueryResultProcessor.generateLog(queryResult, ltidToIsbn, aipm);
		System.out.println(log);
		assertTrue(log.length() > 0);
	}

}
