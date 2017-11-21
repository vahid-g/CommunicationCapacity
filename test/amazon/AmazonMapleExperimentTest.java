package amazon;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import query.ExperimentQuery;
import query.Qrel;
import query.QueryResult;
import amazon.query.AmazonQueryResultProcessor;

public class AmazonMapleExperimentTest {

	Map<String, String> isbnToLtid = new HashMap<String, String>();
	QueryResult queryResult;
	TreeSet<String> cache = new TreeSet<String>();

	@Before
	public void init() {
		Set<Qrel> qrelSet = new HashSet<Qrel>();
		qrelSet.add(new Qrel(1, "lt1", 1));
		qrelSet.add(new Qrel(1, "lt2", 1));
		qrelSet.add(new Qrel(2, "lt2", 1));
		qrelSet.add(new Qrel(2, "lt3", 1));
		ExperimentQuery eq = new ExperimentQuery(1, "q1 text", qrelSet);
		queryResult = new QueryResult(eq);
		queryResult.addResult("is1", "title1");
		queryResult.addResult("is2", "title2");
		queryResult.addResult("is3", "title3");
		queryResult.addResult("is4", "title4");
		isbnToLtid.put("is1", "lt1");
		isbnToLtid.put("is2", "lt1");
		isbnToLtid.put("is3", "lt1");
		isbnToLtid.put("is4", "lt2");
		isbnToLtid.put("is5", "lt3");
		Set<String> set1 = new HashSet<String>();
		set1.add("is1");
		set1.add("is2");
		set1.add("is3");
		Set<String> set2 = new HashSet<String>();
		set2.add("is4");
		Set<String> set3 = new HashSet<String>();
		set3.add("is5");
	}

	@Test
	public void testFilterCachedResults(){
		AmazonQueryResultProcessor.convertIsbnAnswersToLtidAndFilter(
				queryResult, isbnToLtid);
		cache.add("lt2");
		cache.add("lt3");
		AmazonMapleExperiment.filterCacheResults(queryResult, cache);
		assertEquals(1, queryResult.getTopDocuments().size());
	}
	
	@Test
	public void testLoadIsbnList() throws IOException{
		List<String> isbns = AmazonMapleExperiment.loadIsbnList("test_data/path.csv");
		assertEquals(5, isbns.size());
		assertEquals("i1", isbns.get(0));
		
	}

}
