package amazon;

import static org.junit.Assert.*;

import org.junit.Test;

public class AmazonIsbnConverterTest{

	AmazonIsbnConverter converter = AmazonIsbnConverter.getInstance("data/amazon_data/amazon-lt.isbn.thingID.csv");
	
	@Test
	public void testConvertIsbnToLtid() {
		String isbn = "1577551672";
		String ltid = "7729670";
		assertEquals(ltid, converter.convertIsbnToLtid(isbn));
	}

}
