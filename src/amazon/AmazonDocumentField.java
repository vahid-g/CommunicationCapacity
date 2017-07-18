package amazon;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public enum AmazonDocumentField {
	TITLE("title"), CONTENT("content"), CREATOR("creator"), TAGS("tags"), DEWEY("dewey");

	private static final Logger LOGGER = Logger.getLogger(AmazonDocumentField.class.getName());

	private final String label;

	private int missingDeweyCounter = 0;

	AmazonDocumentField(final String s) {
		label = s;
	}

	public String toString() {
		return label;
	}

	public int getMissingDeweyCounter() {
		return missingDeweyCounter;
	}

	public String extractNodeTextFromXml(org.w3c.dom.Document xmlDoc, Node bookNode) {
		String text = "";
		switch (this) {
		case TITLE:
			text = extractNodeTextFromXml(xmlDoc, this.toString(), bookNode);
			break;
		case CREATOR:
			text = extractNodeTextFromXml(xmlDoc, this.toString(), bookNode);
			break;
		case TAGS:
			text = extractNodeTextFromXml(xmlDoc, this.toString(), bookNode);
			// text = extractNormalizedTagNode(xmlDoc, this.toString(), bookNode);
			break;
		case DEWEY:
			Map<String, String> deweyToCategory = loadDeweyMap(AmazonDirectoryInfo.DEWEY_DICT);
			String dewey = extractNodeTextFromXml(xmlDoc, "dewey", bookNode);
			if (dewey.contains("."))
				dewey = dewey.substring(0, dewey.indexOf('.'));
			if (deweyToCategory.containsKey(dewey.trim())) {
				text = deweyToCategory.get(dewey.trim());
			} else {
				missingDeweyCounter++;
			}
			break;
		case CONTENT:
			bookNode.normalize();
			text = bookNode.getTextContent();
			break;
		default:
			LOGGER.log(Level.SEVERE, "Parser is not implemented for field type: " + this.toString());
		}
		return text;
	}

	private String extractNodeTextFromXml(org.w3c.dom.Document xmlDoc, String nodeName, Node bookNode) {
		String nodeText = "";
		try {
			NodeList nodeList = xmlDoc.getElementsByTagName(nodeName);
			Node node = nodeList.item(0);
			nodeText = node.getTextContent().trim();
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

	@SuppressWarnings("unused") // Not used since it decreases the effectiveness
	private String extractNormalizedTagNode(org.w3c.dom.Document xmlDoc, String nodeName, Node bookNode) {
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

	private static Map<String, String> loadDeweyMap(String path) {
		LOGGER.log(Level.INFO, "Loading Dewey dictionary..");
		Map<String, String> deweyMap = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			String line = br.readLine();
			while (line != null) {
				String[] fields = line.split("   ");
				// adds dewey id --> text category
				deweyMap.put(fields[0].trim(), fields[1].trim());
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO, "Dewey dictionary loaded. Size: " + deweyMap.size());
		return deweyMap;
	}
}
