package amazon;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

import amazon.datatools.AmazonIsbnConverter;

public class AmazonIsbnConverterTest {

	@Test
	public void testIsbnToLtid() {
		Map<String, String> converter = AmazonIsbnConverter
				.loadIsbnToLtidMap("data/amazon/queries/amazon-lt.isbn.thingID.csv");
		String isbn = "1577551672";
		String ltid = "7729670";
		assertEquals(ltid, converter.get(isbn));
	}

	public void testLtidToIsbn() {
		Map<String, Set<String>> ltidToIsbns = AmazonIsbnConverter
				.loadLtidToIsbnMap("data/amazon/queries/amazon-lt.isbn.thingID.csv");
		String ltid = "195721";
		assertEquals(1, ltidToIsbns.get(ltid).size());
	}

}
