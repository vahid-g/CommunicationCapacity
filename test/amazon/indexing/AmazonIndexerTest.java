package amazon.indexing;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.sun.org.apache.xerces.internal.dom.DocumentImpl;

import amazon.AmazonDocumentField;
import junit.framework.TestCase;

public class AmazonIndexerTest extends TestCase {

	AmazonDocumentField[] fields = { AmazonDocumentField.TITLE, AmazonDocumentField.CONTENT,
			AmazonDocumentField.CREATORS, AmazonDocumentField.TAGS, AmazonDocumentField.DEWEY };

	AmazonIndexer indexer;

	public void setUp() {
		Map<String, String> isbnToLtid = new HashMap<String, String>();
		isbnToLtid.put("1931243999", "ltid");
		indexer = new AmazonIndexer(fields, isbnToLtid, "data/amazon_data/dewey.csv");
	}

	@Test
	public void testExtractNodeTextContent() {
		Document xmlDoc = new DocumentImpl();
		Element root = xmlDoc.createElement("book");
		Node innerItem = xmlDoc.createElement("innerItem");
		innerItem.appendChild(xmlDoc.createTextNode("innerText"));
		Node item = xmlDoc.createElement("outerItem");
		item.appendChild(innerItem);
		item.appendChild(xmlDoc.createTextNode("outerText"));
		root.appendChild(item);
		xmlDoc.appendChild(root);
		assertEquals("innerText outerText", indexer.extractNodeTextContent(root));
	}

	public void testExtractNodesTextFromXml() throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		File file = new File("data/test_data/1931243999.xml");
		org.w3c.dom.Document xmlDoc = db.parse(file);
		Node bookNode = xmlDoc.getElementsByTagName("book").item(0);
		assertEquals("Journey Around My Room (Green Integer)",
				indexer.extractNodesTextFromXml(xmlDoc, AmazonDocumentField.TITLE, bookNode));
		assertEquals("Mark Axelrod Translator Xavier de Maistre Author",
				indexer.extractNodesTextFromXml(xmlDoc, AmazonDocumentField.CREATORS, bookNode));
		assertEquals("unread literature Fiction",
				indexer.extractNodesTextFromXml(xmlDoc, AmazonDocumentField.TAGS, bookNode));
		assertEquals("910", indexer.extractNodesTextFromXml(xmlDoc, AmazonDocumentField.DEWEY, bookNode));
	}

	public void testParseAmazonXml() {
		File file = new File("data/test_data/1931243999.xml");
		Map<AmazonDocumentField, String> dMap = indexer.parseAmazonXml(file);
		assertEquals("Geography & travel", dMap.get(AmazonDocumentField.DEWEY));
		assertEquals("unread literature Fiction", dMap.get(AmazonDocumentField.TAGS));
	}

}
