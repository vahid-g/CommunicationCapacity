package mslr;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class MslrExperimentTest {

    @Test
    public void testLoadSortedClickCountList() {
	List<Integer> results = MslrExperiment.loadSortedClickCountList("test_data/mslr_sample.txt");
	assertEquals(3, (int) results.get(0));
    }

}
