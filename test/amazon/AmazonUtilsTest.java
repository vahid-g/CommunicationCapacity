package amazon;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Test;

public class AmazonUtilsTest {

	@Test
	public void test() {
		StringReader sr = new StringReader("x,0000055555,2,2\n" + 
				"y,0000055555,3,2");
		StringWriter sw = new StringWriter();
		AmazonUtils.parseRatings(sr, sw);
		assertEquals("555/0000055555.xml,2.5", sw.toString());
	}

}
