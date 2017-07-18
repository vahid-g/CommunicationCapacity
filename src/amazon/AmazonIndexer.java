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
import org.w3c.dom.Node;

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
				String text = field.extractNodeTextFromXml(xmlDoc, bookNode);
				dataMap.put(field, text);
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return dataMap;
	}

}
