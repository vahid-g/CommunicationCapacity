package imdb;

import indexing.GeneralIndexer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ImdbIndexer extends GeneralIndexer {

	static final Logger LOGGER = Logger.getLogger(ImdbIndexer.class.getName());

	public static void main(String[] args) {
		File file = new File(
				"/scratch/data-sets/imdb/imdb-inex/movies/000/947000.xml");
		try (InputStream fis = Files.newInputStream(file.toPath())) {

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document doc = db.parse(file);
			// removing title and actors info
			Element element = (Element) doc.getElementsByTagName("actors")
					.item(0);
			element.getParentNode().removeChild(element);
			element = (Element) doc.getElementsByTagName("title").item(0);
			element.getParentNode().removeChild(element);
			doc.normalize();

			System.out.println(doc.getElementsByTagName("movie").item(0)
					.getTextContent());
			// prettyPrint(doc);
			// String rest = fileContent.replaceAll("<[^>]*>",
			// "").trim().replaceAll(title, "");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static final void prettyPrint(org.w3c.dom.Document xml)
			throws Exception {
		Transformer tf = TransformerFactory.newInstance().newTransformer();
		tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		tf.setOutputProperty(OutputKeys.INDENT, "yes");
		java.io.Writer out = new StringWriter();
		tf.transform(new DOMSource(xml), new StreamResult(out));
		System.out.println(out.toString());
	}

	protected void indexXmlFile(File file, IndexWriter writer, float smoothed,
			float... gamma) {
		if (gamma.length < 2) {
			LOGGER.log(Level.SEVERE, "Not enough gammas!!!");
			return;
		}
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document doc = db.parse(file);
			NodeList nodeList = doc.getElementsByTagName("title");
			Node node = nodeList.item(0).getFirstChild();
			String title = "";
			if (node.getNodeValue() != null) {
				title = node.getNodeValue().trim();
			} else {
				LOGGER.log(Level.WARNING,
						"title not found in: " + file.getName());
			}
			nodeList = doc.getElementsByTagName("actor");
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < nodeList.getLength(); i++) {
				sb.append(nodeList.item(i).getTextContent());
			}
			String actorsInfo = sb.toString();

			try {
				// removing title and actors info
				Element element = (Element) doc.getElementsByTagName("actors")
						.item(0);
				element.getParentNode().removeChild(element);
				element = (Element) doc.getElementsByTagName("title").item(0);
				element.getParentNode().removeChild(element);
			} catch (NullPointerException e) {
				// file doesn't have actors/title
			}
			
			doc.normalize();
			String rest = doc.getElementsByTagName("movie").item(0)
					.getTextContent();

			float gamma3 = 1 - gamma[0] - gamma[1];
			if (gamma3 < 0) {
				LOGGER.log(Level.SEVERE, "gamma3 is less than 0");
				gamma3 = 0;
			}
			Document lDoc = new Document();
			lDoc.add(new StringField(GeneralIndexer.DOCNAME_ATTRIB,
					FilenameUtils.removeExtension(file.getName()),
					Field.Store.YES));
			TextField titleField = new TextField(GeneralIndexer.TITLE_ATTRIB,
					title, Field.Store.YES);
			titleField.setBoost(gamma[0] * smoothed);
			TextField actorsField = new TextField(GeneralIndexer.ACTORS_ATTRIB,
					actorsInfo, Field.Store.YES);
			actorsField.setBoost(gamma[1] * smoothed);
			TextField contentField = new TextField(
					GeneralIndexer.CONTENT_ATTRIB, rest, Field.Store.YES);
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

	protected void indexXmlFileOld(File file, IndexWriter writer,
			float smoothed, float gamma) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			Pattern ptr = Pattern.compile("<title>([^<]*)</title>");
			Matcher mtr = ptr.matcher(fileContent);
			String title = "";
			if (mtr.find()) {
				title = mtr.group(1).trim();
			} else {
				LOGGER.log(Level.WARNING,
						"title not found in: " + file.getName());
			}
			String textContent = fileContent.replaceAll("<[^>]*>", "").trim();
			Document doc = new Document();
			doc.add(new StringField(GeneralIndexer.DOCNAME_ATTRIB,
					FilenameUtils.removeExtension(file.getName()),
					Field.Store.YES));
			TextField titleField = new TextField(GeneralIndexer.TITLE_ATTRIB,
					title, Field.Store.YES);
			titleField.setBoost(gamma * smoothed);
			TextField contentField = new TextField(
					GeneralIndexer.CONTENT_ATTRIB, textContent, Field.Store.YES);
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

}
