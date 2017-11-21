package amazon.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
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
		ltidToIsbn.put("lt1", set1);
		ltidToIsbn.put("lt4", set2);
		ltidToIsbn.put("lt3", set3);
	}

	@Test
	public void testConvertIsbnAnswersToSetAndFilter() {
		AmazonQueryResultProcessor.convertIsbnAnswersToLtidAndFilter(
				queryResult, isbnToLtid);
		assertEquals(2, queryResult.getTopDocuments().size());
		assertEquals("lt1", queryResult.getTopDocuments().get(0).id);
		assertEquals("lt2", queryResult.getTopDocuments().get(1).id);
	}

	@Test
	public void testGenerateLog() throws IOException {
		AmazonQueryResultProcessor.convertIsbnAnswersToLtidAndFilter(
				queryResult, isbnToLtid);
		AmazonIsbnPopularityMap aipm = AmazonIsbnPopularityMap
				.getInstance("test_data/path.csv");
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get("data/index")));
		String log = AmazonQueryResultProcessor.generateLog(queryResult,
				ltidToIsbn, aipm, reader);
		System.out.println(log);
		assertTrue(log.length() > 0);
	}

}
