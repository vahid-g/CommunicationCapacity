package mslr;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
    }

    @Test
    public void testGetValueOfKeyValueString() {
	String keyValue = "key:value";
	assertEquals("value",
		MslrExperiment.getValueOfKeyValueString(keyValue));
	assertEquals("", MslrExperiment.getValueOfKeyValueString(":"));
    }

}
