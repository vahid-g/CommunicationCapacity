package mslr;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class MslrExperimentTest {

    @Test
    public void test() {
	List<Integer> results = MslrExperiment.loadSortedClickCountList();
	assertEquals(3, (int)results.get(0));
    }

}
