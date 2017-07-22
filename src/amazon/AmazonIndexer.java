package amazon;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import indexing.GeneralIndexer;

public class AmazonIndexer extends GeneralIndexer {

	public static final String LTID_ATTRIB = "ltid";

	private static final Logger LOGGER = Logger.getLogger(AmazonIndexer.class.getName());

	private AmazonDocumentField[] fields;

	public AmazonIndexer(AmazonDocumentField[] fields) {
		this.fields = fields;

	}

	@Override
	protected void indexXmlFile(File file, IndexWriter writer, float docBoost, float[] fieldBoost) {
		try {
			Map<AmazonDocumentField, String> dataMap = parseAmazonXml(file);
			Document luceneDoc = new Document();
			// file name is ISBN of the book
			String docId = FilenameUtils.removeExtension(file.getName());
			String ltid = AmazonExperiment.isbnToLtid.get(docId);
			luceneDoc.add(new StringField(DOCNAME_ATTRIB, docId, Field.Store.YES));
			luceneDoc.add(new StringField(LTID_ATTRIB, ltid, Field.Store.YES));
			for (AmazonDocumentField field : fields) {
				TextField textField = new TextField(field.toString(), dataMap.get(field), Field.Store.YES);
				luceneDoc.add(textField);
			}
			writer.addDocument(luceneDoc);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	protected HashMap<AmazonDocumentField, String> parseAmazonXml(File file) {
		HashMap<AmazonDocumentField, String> dataMap = new HashMap<AmazonDocumentField, String>();
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document xmlDoc = db.parse(file);
			Node bookNode = xmlDoc.getElementsByTagName("book").item(0);
			for (AmazonDocumentField field : fields) {
				if (field == AmazonDocumentField.CONTENT)
					continue;
				String text = extractNodesTextFromXml(xmlDoc, field, bookNode);
				if (field == AmazonDocumentField.DEWEY)
					text = AmazonDeweyConvertor.getInstance().convertDeweyToCategory(text);
				dataMap.put(field, text);
			}
			String rest = extractNodeTextContent(bookNode);
			dataMap.put(AmazonDocumentField.CONTENT, rest);
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

		return dataMap;
	}

	protected String extractNodesTextFromXml(org.w3c.dom.Document xmlDoc, AmazonDocumentField docField, Node bookNode) {
		String nodeName = docField.toString();
		String nodeText = "";
		try {
			NodeList nodeList = xmlDoc.getElementsByTagName(nodeName);
			Node node = nodeList.item(0);
			nodeText = extractNodeTextContent(node);
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

	// Not used since it decreases the effectiveness
	protected String extractNormalizedTagNode(org.w3c.dom.Document xmlDoc, String nodeName, Node bookNode) {
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

	protected String extractNodeTextContent(Node node) {
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

}
