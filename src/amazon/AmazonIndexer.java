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
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import indexing.GeneralIndexer;

public class AmazonIndexer extends GeneralIndexer {

	static final Logger LOGGER = Logger.getLogger(AmazonIndexer.class.getName());

	public static final String CREATOR_ATTRIB = "creator";

	public static final String TAGS_ATTRIB = "tags";
	
	public static void main(String[] args) {
	}
	
	@Override
	protected void indexXmlFile(File file, IndexWriter writer, float docBoost, float[] fieldBoost) {
		try {
			Map<String, String> dataMap = parseAmazonXml(file);
			Document luceneDoc = new Document();
			// file name is ISBN of the book
			String docId = FilenameUtils.removeExtension(file.getName());
			// String ltid = AmazonExperiment.isbnToLtid.get(docId);
			luceneDoc.add(new StringField(DOCNAME_ATTRIB, docId, Field.Store.YES));
			// luceneDoc.add(new StringField(LTID_ATTRIB, ltid,
			// Field.Store.YES));
			TextField titleField = new TextField(TITLE_ATTRIB, dataMap.get(TITLE_ATTRIB), Field.Store.YES);
			luceneDoc.add(titleField);
			TextField genreField = new TextField(CREATOR_ATTRIB, dataMap.get(CREATOR_ATTRIB), Field.Store.YES);
			luceneDoc.add(genreField);
			TextField tagsField = new TextField(TAGS_ATTRIB, dataMap.get(TAGS_ATTRIB), Field.Store.YES);
			luceneDoc.add(tagsField);
			TextField restField = new TextField(CONTENT_ATTRIB, dataMap.get(CONTENT_ATTRIB), Field.Store.YES);
			luceneDoc.add(restField);

			writer.addDocument(luceneDoc);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	protected static HashMap<String, String> parseAmazonXml(File file) {
		HashMap<String, String> dataMap = new HashMap<String, String>();
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document xmlDoc = db.parse(file);
			
			NodeList titleNodeList = xmlDoc.getElementsByTagName("title");
			Node titleNode = titleNodeList.item(0);
			String title = "";
			if (titleNode != null) {
				title = titleNode.getTextContent();
			} else {
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}
			dataMap.put(TITLE_ATTRIB, title);

			NodeList creatorNodeList = xmlDoc.getElementsByTagName("creators");
			Node creatorNode = creatorNodeList.item(0);
			String creators = "";
			if (creatorNode != null) {
				creators = creatorNode.getTextContent() + " ";
			}
			dataMap.put(CREATOR_ATTRIB, creators);

			NodeList tagList = xmlDoc.getElementsByTagName("tag");
			StringBuilder sb = new StringBuilder();
			if (tagList != null) {
				for (int i = 0; i < tagList.getLength(); i++) {
					Node tagNode = tagList.item(i);
					Element tagElement = (Element) tagNode;
					// normalizes the tag texts according to their frequency count
					int freq = Integer.parseInt(tagElement.getAttribute("count"));
					while (freq-- > 0) {
						sb.append(tagNode.getTextContent() + " ");
					}
				}
			}
			dataMap.put(TAGS_ATTRIB, sb.toString());

			// removing title and actors info
			String rest = "";
			try {
				Node bookNode = xmlDoc.getElementsByTagName("book").item(0);
				bookNode.removeChild(titleNode);
				bookNode.removeChild(creatorNode);
				bookNode.removeChild(xmlDoc.getElementsByTagName("tags").item(0));
				bookNode.normalize();
				rest = bookNode.getTextContent();
			} catch (NullPointerException e) {
				LOGGER.log(Level.WARNING, "rest (CONTENT_ATTRIB) is null for: " + file.getName());
				LOGGER.log(Level.WARNING, e.getMessage(), e);
			}
			dataMap.put(CONTENT_ATTRIB, rest);
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return dataMap;
	}
}
