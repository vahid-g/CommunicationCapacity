package amazon;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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

	static final Logger LOGGER = Logger.getLogger(AmazonIndexer.class.getName());

	private static Map<String, String> deweyToCategory = loadDeweyMap(AmazonDirectoryInfo.DEWEY_DICT);
	
	private static int missingDeweyCounter = 0;
	
	@Override
	protected void indexXmlFile(File file, IndexWriter writer, float docBoost, float[] fieldBoost) {
		try {
			Map<AmazonDocumentField, String> dataMap = parseAmazonXml(file);
			Document luceneDoc = new Document();
			// file name is ISBN of the book
			String docId = FilenameUtils.removeExtension(file.getName());
			// String ltid = AmazonExperiment.isbnToLtid.get(docId);
			luceneDoc.add(new StringField(DOCNAME_ATTRIB, docId, Field.Store.YES));
			// luceneDoc.add(new StringField(LTID_ATTRIB, ltid,
			// Field.Store.YES));
			for (AmazonDocumentField field : AmazonDocumentField.values()){
				TextField textField = new TextField(field.toString(), dataMap.get(field), Field.Store.YES);
				luceneDoc.add(textField);
			}
			writer.addDocument(luceneDoc);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	protected static HashMap<AmazonDocumentField, String> parseAmazonXml(File file) {
		HashMap<AmazonDocumentField, String> dataMap = new HashMap<AmazonDocumentField, String>();
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document xmlDoc = db.parse(file);
			Node bookNode = xmlDoc.getElementsByTagName("book").item(0);

			String title = extractNodeFromXml(xmlDoc, "title", bookNode);
			dataMap.put(AmazonDocumentField.TITLE, title);

			String creators = extractNodeFromXml(xmlDoc, "creators", bookNode);
			dataMap.put(AmazonDocumentField.CREATOR, creators);

			String tags = "";
			try {
				NodeList tagList = xmlDoc.getElementsByTagName("tag");
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < tagList.getLength(); i++) {
					Node tagNode = tagList.item(i);
					Element tagElement = (Element) tagNode;
					// normalizes the tag texts according to their frequency
					// count
					int freq = Integer.parseInt(tagElement.getAttribute("count"));
					while (freq-- > 0) {
						sb.append(tagNode.getTextContent() + " ");
					}
				}
				tags = sb.toString();
			} catch (NullPointerException npe) {
				LOGGER.log(Level.WARNING, "Null Pointer: couldn't extract tags");
			}
			dataMap.put(AmazonDocumentField.TAGS, tags);

			String category = "";
			String dewey = extractNodeFromXml(xmlDoc, "dewey", bookNode);
			if (dewey.contains("."))
				dewey = dewey.substring(0, dewey.indexOf('.'));
			if (deweyToCategory.containsKey(dewey.trim())) {
				category = deweyToCategory.get(dewey.trim());
			} else {
				//LOGGER.log(Level.WARNING, "deweyDict doesn't contain " + dewey.trim() + 
					//	" \n\tfile: " + file.getAbsolutePath());
				missingDeweyCounter++;
			}
			dataMap.put(AmazonDocumentField.DEWEY, category);

			// removing title and actors info
			String rest = "";
			bookNode.normalize();
			rest = bookNode.getTextContent();
			dataMap.put(AmazonDocumentField.CONTENT, rest);
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return dataMap;
	}

	public static int getMissingDeweyCounter() {
		return missingDeweyCounter;
	}

	private static String extractNodeFromXml(org.w3c.dom.Document xmlDoc, String nodeName, Node bookNode) {
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
		LOGGER.log(Level.INFO, "Dewey dictionary load. Size: " + deweyMap.size());
		return deweyMap;
	}
}
