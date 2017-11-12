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
				.loadIsbnToLtidMap("test_data/isbn_ltid.csv");
		String isbn = "0330308297";
		String ltid = "17";
		assertEquals(ltid, converter.get(isbn));
	}

	public void testLtidToIsbn() {
		Map<String, Set<String>> ltidToIsbns = AmazonIsbnConverter
				.loadLtidToIsbnMap("data/amazon/queries/amazon-lt.isbn.thingID.csv");
		String ltid = "17";
		assertEquals(5, ltidToIsbns.get(ltid).size());
	}

}
