package amazon.utils;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.sun.org.apache.xerces.internal.dom.DocumentImpl;

import amazon.AmazonDocumentField;
import junit.framework.TestCase;

public class AmazonXmlUtilsTest extends TestCase {

	@Test
	public void testExtractNodesTextFromXml() throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		File file = new File("test_data/1931243999.xml");
		org.w3c.dom.Document xmlDoc = db.parse(file);
		Node bookNode = xmlDoc.getElementsByTagName("book").item(0);
		assertEquals("Journey Around My Room (Green Integer)",
				AmazonXmlUtils.extractNodesTextFromXml(xmlDoc,
						AmazonDocumentField.TITLE, bookNode));
		assertEquals("Mark Axelrod Translator Xavier de Maistre Author",
				AmazonXmlUtils.extractNodesTextFromXml(xmlDoc,
						AmazonDocumentField.CREATORS, bookNode));
		assertEquals("unread literature Fiction",
				AmazonXmlUtils.extractNodesTextFromXml(xmlDoc,
						AmazonDocumentField.TAGS, bookNode));
		assertEquals("910", AmazonXmlUtils.extractNodesTextFromXml(xmlDoc,
				AmazonDocumentField.DEWEY, bookNode));
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
		assertEquals("innerText outerText",
				AmazonXmlUtils.extractNodeTextContent(root));
	}

}
