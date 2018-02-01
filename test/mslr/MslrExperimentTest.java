package mslr;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import mslr.MslrExperiment.QueryResult;

public class MslrExperimentTest {

    @Test
    public void testLoadSortedClickCountList() {
	List<Integer> results = MslrExperiment
		.loadSortedClickCountList("test_data/mslr_sample.txt");
	assertEquals(10, (int) results.get(0));
    }

    @Test
    public void testLoadQueryResults() throws IOException {
	BufferedReader br = new BufferedReader(
		new FileReader("test_data/mslr_sample.txt"));
	Map<Integer, List<QueryResult>> qidResults = MslrExperiment
		.loadQueryResults(br);
	assertEquals(1, qidResults.keySet().size());
	assertEquals(10, qidResults.get(1).size());
	assertEquals(20.88, qidResults.get(1).get(0).bm25, 0.01);
    }

    @Test
    public void testGetValueOfKeyValueString() {
	String keyValue = "key:value";
	assertEquals("value",
		MslrExperiment.getValueOfKeyValueString(keyValue));
	assertEquals("", MslrExperiment.getValueOfKeyValueString(":"));
    }

    @Test
    public void testPrecisionAtK() {
	List<QueryResult> resultList = new ArrayList<QueryResult>();
	resultList.add(new QueryResult(0.9, 5, 10));
	resultList.add(new QueryResult(0.8, 5, 10));
	resultList.add(new QueryResult(0.8, 1, 10));
	resultList.add(new QueryResult(0.8, 0, 10));
	assertEquals(0.2, MslrExperiment.precisionAtK(resultList, 10), 0.01);
    }

    @Test
    public void testGetThresholds() {
	Integer[] clickCountsArray = { 10, 9, 5, 1, 0 };
	List<Integer> thresholds = MslrExperiment
		.buildThresholdsList(Arrays.asList(clickCountsArray), 2);
	assertEquals(2, thresholds.size());
	assertEquals(5, (int) thresholds.get(0));
	assertEquals(0, (int) thresholds.get(1));
    }

}
