package amazon;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Map;

import org.junit.Test;

public class AmazonIndexerTest {

	AmazonDocumentField[] fields = { AmazonDocumentField.TITLE, AmazonDocumentField.CONTENT,
			AmazonDocumentField.CREATOR, AmazonDocumentField.TAGS, AmazonDocumentField.DEWEY };

	@Test
	public void testParseAmazonXml() {
		File file = new File("data/test_data/1931243999.xml");
		AmazonIndexer indexer = new AmazonIndexer(fields);
		Map<AmazonDocumentField, String> dMap = indexer.parseAmazonXml(file);
		assertEquals("unread Fiction Fiction", dMap.get(AmazonDocumentField.TAGS).trim());
		assertEquals("Geography & travel", dMap.get(AmazonDocumentField.DEWEY));
		assertEquals("Journey Around My Room (Green Integer)", dMap.get(AmazonDocumentField.TITLE));
	}

}
