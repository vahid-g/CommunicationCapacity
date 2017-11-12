package popularity;

import java.util.Map;

import org.junit.Test;

public class PopularityUtilsTest {

	@Test
	public void testLoadIsbnRatingsMap() {
		Map<String, Double> idPopMap = PopularityUtils.loadIdPopularityMap("test_data/wiki_accesscount.csv");
		
		
	}

}
