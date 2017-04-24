package imdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import wiki_inex09.Wiki09Indexer;

public class ImdbIndexer extends GeneralIndexer {
	
	static final Logger LOGGER = Logger.getLogger(ImdbIndexer.class.getName());

	@Override
	protected void indexXmlFile(File file, IndexWriter writer, float smoothed,
			float gamma) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			Pattern ptr = Pattern.compile("<title>([^<]*)</title>");
			Matcher mtr = ptr.matcher(fileContent);
			String title = "";
			if (mtr.find()){
				title = mtr.group(1).trim();
			} else {
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}
			String textContent = fileContent.replaceAll("<[^>]*>", "").trim();
			Document doc = new Document();
			doc.add(new StringField(Wiki09Indexer.DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(Wiki09Indexer.TITLE_ATTRIB, title, Field.Store.YES);
			titleField.setBoost(gamma * smoothed);
			TextField contentField = new TextField(Wiki09Indexer.CONTENT_ATTRIB, textContent,
					Field.Store.YES);
			contentField.setBoost((1 - gamma) * smoothed);
			doc.add(titleField);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected void indexXmlFile(File file, IndexWriter writer, float smoothed, float gamma, float gamma2) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document doc = db.parse(file);
			NodeList nodeList = doc.getElementsByTagName("title");
			Node node = nodeList.item(0).getFirstChild();
			String title = "";
			if (node.getNodeValue() != null) {
				title = node.getNodeValue().trim();
			} else {
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}
			nodeList = doc.getElementsByTagName("actor");
			StringBuilder sb = new StringBuilder();
			for (int i =0; i < nodeList.getLength(); i++){
				sb.append(nodeList.item(i).getTextContent());
			}
			String actorsInfo = sb.toString();
			String rest = fileContent.replaceAll("<[^>]*>", "").trim();

			float gamma3 = 1 - gamma - gamma2;
			if (gamma3 < 0){
				LOGGER.log(Level.SEVERE, "gamma3 is less than 0");
				gamma3 = 0;
			}
			Document lDoc = new Document();
			lDoc.add(new StringField(Wiki09Indexer.DOCNAME_ATTRIB, FilenameUtils.removeExtension(file.getName()),
					Field.Store.YES));
			TextField titleField = new TextField(Wiki09Indexer.TITLE_ATTRIB, title, Field.Store.YES);
			titleField.setBoost(gamma * smoothed);
			TextField actorsField = new TextField("another_attirb", actorsInfo, Field.Store.YES);
			actorsField.setBoost(gamma2 * smoothed);
			TextField contentField = new TextField(Wiki09Indexer.CONTENT_ATTRIB, rest, Field.Store.YES);
			contentField.setBoost(gamma3 * smoothed);
			lDoc.add(titleField);
			lDoc.add(actorsField);
			lDoc.add(contentField);
			writer.addDocument(lDoc);
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
