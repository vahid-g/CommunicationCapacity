package amazon;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import indexing.GeneralIndexer;

public class AmazonIndexer extends GeneralIndexer {

	static final Logger LOGGER = Logger.getLogger(AmazonIndexer.class.getName());

	public static final String CREATOR_ATTRIB = "creator";

	@Override
	protected void indexXmlFile(File file, IndexWriter writer, float docBoost, float[] fieldBoost) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document xmlDoc = db.parse(file);

			NodeList titleNodeList = xmlDoc.getElementsByTagName("title");
			Node titleNode = titleNodeList.item(0);
			String title = "";
			if (titleNode != null) {
				title = titleNode.getNodeValue();
			} else {
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}

			NodeList creatorNodeList = xmlDoc.getElementsByTagName("creators");
			Node creatorNode = creatorNodeList.item(0);
			String creators = "";
			if (creatorNode != null) {
				creators = creatorNode.getTextContent();
			}

			xmlDoc.removeChild(titleNode);
			xmlDoc.removeChild(creatorNode);
			xmlDoc.normalize();
			String rest = xmlDoc.getTextContent();

			Document luceneDoc = new Document();
			luceneDoc.add(
					new StringField(DOCNAME_ATTRIB, FilenameUtils.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(TITLE_ATTRIB, title, Field.Store.YES);
			luceneDoc.add(titleField);
			TextField genreField = new TextField(CREATOR_ATTRIB, creators, Field.Store.YES);
			luceneDoc.add(genreField);
			TextField restField = new TextField(CONTENT_ATTRIB, rest, Field.Store.YES);
			luceneDoc.add(restField);

			writer.addDocument(luceneDoc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}

	}

}
