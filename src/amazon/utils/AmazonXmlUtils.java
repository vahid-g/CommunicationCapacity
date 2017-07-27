package amazon.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import amazon.AmazonDocumentField;

public class AmazonXmlUtils {
	
	private static final Logger LOGGER = Logger.getLogger(AmazonXmlUtils.class.getName());
	
	public static String extractNodesTextFromXml(org.w3c.dom.Document xmlDoc, AmazonDocumentField docField, Node bookNode) {
		String nodeName = docField.toString();
		String nodeText = "";
		try {
			NodeList nodeList = xmlDoc.getElementsByTagName(nodeName);
			Node node = nodeList.item(0);
			nodeText = AmazonXmlUtils.extractNodeTextContent(node);
			try {
				bookNode.removeChild(node);
			} catch (DOMException dome) {
				LOGGER.log(Level.WARNING, "Couldn't remove node " + nodeText);
			}
		} catch (NullPointerException npe) {
			LOGGER.log(Level.WARNING, "Null Pointer: couldn't extract dewey " + nodeName);
		}
		return nodeText;
	}

	public static String extractNodeTextContent(Node node) {
		NodeList kids = node.getChildNodes();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < kids.getLength(); i++) {
			Node kid = kids.item(i);
			if (kid.getNodeType() == Node.TEXT_NODE) {
				sb.append(kid.getNodeValue().trim());
			} else {
				sb.append(extractNodeTextContent(kid));
				sb.append(' ');
			}
		}
		return sb.toString().trim();
	}

	// Not used since it decreases the effectiveness
	public static String extractNormalizedTagNode(org.w3c.dom.Document xmlDoc, String nodeName, Node bookNode) {
		String tags = "";
		try {
			NodeList tagList = xmlDoc.getElementsByTagName("tag");
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < tagList.getLength(); i++) {
				Node tagNode = tagList.item(i);
				Element tagElement = (Element) tagNode;
				int freq = Integer.parseInt(tagElement.getAttribute("count"));
				while (freq-- > 0) {
					sb.append(tagNode.getTextContent() + " ");
				}
			}
			tags = sb.toString();
			NodeList tagsList = xmlDoc.getElementsByTagName("tags");
			Node tagsNode = tagsList.item(0);
			bookNode.removeChild(tagsNode);
		} catch (NullPointerException npe) {
			LOGGER.log(Level.WARNING, "Null Pointer: couldn't extract tags");
		}
		return tags;
	}

}
