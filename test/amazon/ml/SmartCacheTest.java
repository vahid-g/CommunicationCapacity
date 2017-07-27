package amazon.ml;

import java.io.File;
import java.util.List;

import org.junit.Test;

import junit.framework.TestCase;

public class SmartCacheTest extends TestCase{

	@Test
	public void testExtractFeatureVector() {
		File file = new File("data/test_data/1931243999.xml");
		List<String> result = new SmartCache().extractFeatureVector(file);
		assertEquals("Paperback", result.get(0)); // binding
		assertEquals("10.95", result.get(1)); // price
		assertEquals("2005", result.get(2)); // year
		assertEquals("910", result.get(3)); // dewey
		assertEquals("200", result.get(4)); // pages
		
		assertEquals("4", result.get(5)); // images
		assertEquals("1", result.get(6)); // reviews
		assertEquals("1", result.get(7)); // editorial reviews
		assertEquals("2", result.get(8)); // creators
		assertEquals("5", result.get(9)); // similar prods
		assertEquals("16", result.get(10)); // browse nodes
	}

}
