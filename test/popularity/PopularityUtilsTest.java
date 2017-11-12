package popularity;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

public class PopularityUtilsTest {

	@Test
	public void testLoadIsbnRatingsMap() {
		Map<String, Double> idPopMap = PopularityUtils.loadIdPopularityMap("test_data/wiki_accesscount.csv");
		Double pop = idPopMap.get("15580374");
		assertEquals(579193830.0, pop, 0.001);
	}

}
