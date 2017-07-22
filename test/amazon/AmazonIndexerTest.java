package amazon;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.sun.org.apache.xerces.internal.dom.DocumentImpl;

import amazon.indexing.AmazonIndexer;
import junit.framework.TestCase;

public class AmazonIndexerTest extends TestCase {

	AmazonDocumentField[] fields = { AmazonDocumentField.TITLE, AmazonDocumentField.CONTENT,
			AmazonDocumentField.CREATORS, AmazonDocumentField.TAGS, AmazonDocumentField.DEWEY };

	AmazonIndexer indexer;

	public void setUp() {
		indexer = new AmazonIndexer(fields);
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

	public void testExtractNodesTextFromXml() throws ParserConfigurationException, SAXException, IOException {
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
		AmazonIndexer indexer = new AmazonIndexer(fields);
		AmazonDeweyConvertor.mapPath = "data/amazon_data/dewey.csv";
		Map<AmazonDocumentField, String> dMap = indexer.parseAmazonXml(file);
		assertEquals("Geography & travel", dMap.get(AmazonDocumentField.DEWEY));
	}

}
