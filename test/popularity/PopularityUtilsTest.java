package popularity;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

public class PopularityUtilsTest {

    @Test
    public void testLoadIsbnRatingsMap() {
	Map<String, Double> idPopMap = PopularityUtils
		.loadIdPopularityMap("test_data/wiki_accesscount.csv");
	Double pop = idPopMap.get("15580374");
	assertEquals(579193830.0, pop, 0.001);
	idPopMap = PopularityUtils
		.loadIdPopularityMap("test_data/amazon_integ.csv");
	pop = idPopMap.get("0812532538");
	assertEquals(3655.0, pop, 0.001);
    }
}
