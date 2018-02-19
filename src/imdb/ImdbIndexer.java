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
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import indexing.GeneralIndexer;

public class ImdbIndexer extends GeneralIndexer {

	public static final String KEYWORDS_ATTRIB = "keywords";
	public static final String PLOT_ATTRIB = "plot";
	public static final String ACTORS_ATTRIB = "actors";
	public static final String REST_ATTRIB = "rest";

	static final Logger LOGGER = Logger.getLogger(ImdbIndexer.class.getName());

	public static void main(String[] args) {
		File file = new File("/scratch/data-sets/imdb/imdb-inex/movies/474/1437474.xml");
		try (InputStream fis = Files.newInputStream(file.toPath())) {

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document doc = db.parse(file);

			NodeList titleNodeList = doc.getElementsByTagName("title");
			Node titleNode = titleNodeList.item(0).getFirstChild();
			String title = "";
			if (titleNode.getNodeValue() != null) {
				title = titleNode.getNodeValue().trim();
			} else {
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}
			System.out.println("title: " + title.trim());

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

	public static String[] getAttribList() {
		String[] attribs = { TITLE_ATTRIB, KEYWORDS_ATTRIB, PLOT_ATTRIB, ACTORS_ATTRIB, REST_ATTRIB };
		return attribs;
	}

	protected void indexXmlFile(File file, IndexWriter writer, float docBoost) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document xmlDoc = db.parse(file);

			NodeList titleNodeList = xmlDoc.getElementsByTagName("title");
			Node titleNode = titleNodeList.item(0).getFirstChild();
			String title = "";
			if (titleNode.getNodeValue() != null) {
				title = titleNode.getNodeValue();
			} else {
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}

			StringBuilder sb = new StringBuilder();
			NodeList genreNodeList = xmlDoc.getElementsByTagName("genre");
			if (genreNodeList.item(0) != null) {
				sb.append(genreNodeList.item(0).getTextContent());
			}
			Node keywordsNode = xmlDoc.getElementsByTagName("keywords").item(0);
			if (keywordsNode != null)
				sb.append(keywordsNode.getTextContent());
			String keywords = sb.toString();

			sb = new StringBuilder();
			NodeList plotNodeList = xmlDoc.getElementsByTagName("plot");
			if (plotNodeList.item(0) != null)
				sb.append(plotNodeList.item(0).getTextContent());
			NodeList tagLineNodeList = xmlDoc.getElementsByTagName("tagline");
			if (tagLineNodeList.item(0) != null)
				sb.append(tagLineNodeList.item(0).getTextContent());
			String plotTagLines = sb.toString();

			sb = new StringBuilder();
			Node actors = xmlDoc.getElementsByTagName("actors").item(0);
			if (actors != null)
				sb.append(actors.getTextContent());
			Node composers = xmlDoc.getElementsByTagName("compoer").item(0);
			if (composers != null)
				sb.append(composers.getTextContent());
			Node producers = xmlDoc.getElementsByTagName("producers").item(0);
			if (producers != null)
				sb.append(producers.getTextContent());
			Node writers = xmlDoc.getElementsByTagName("writers").item(0);
			if (writers != null)
				sb.append(writers.getTextContent());
			Node directors = xmlDoc.getElementsByTagName("directors").item(0);
			if (directors != null)
				sb.append(directors.getTextContent());
			String people = sb.toString();

			sb = new StringBuilder();
			NodeList miscNodeList = xmlDoc.getElementsByTagName("miscellaneous");
			if (miscNodeList.item(0) != null)
				sb.append(miscNodeList.item(0).getTextContent());
			NodeList additionalNodeList = xmlDoc.getElementsByTagName("additional_details");
			if (additionalNodeList.item(0) != null)
				sb.append(additionalNodeList.item(0).getTextContent());
			NodeList funNodeList = xmlDoc.getElementsByTagName("fun_stuff");
			if (funNodeList.item(0) != null)
				sb.append(funNodeList.item(0).getTextContent());
			String rest = sb.toString();

			Document luceneDoc = new Document();
			luceneDoc.add(
					new StringField(DOCNAME_ATTRIB, FilenameUtils.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(TITLE_ATTRIB, title, Field.Store.YES);
			luceneDoc.add(titleField);
			TextField genreField = new TextField(KEYWORDS_ATTRIB, keywords, Field.Store.YES);
			luceneDoc.add(genreField);
			TextField plotField = new TextField(PLOT_ATTRIB, plotTagLines, Field.Store.YES);
			luceneDoc.add(plotField);
			TextField peopleField = new TextField(ACTORS_ATTRIB, people, Field.Store.YES);
			luceneDoc.add(peopleField);
			TextField restField = new TextField(REST_ATTRIB, rest, Field.Store.YES);
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

	protected void indexXmlFile3(File file, IndexWriter writer, float smoothed, float... gamma) {
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
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}
			nodeList = doc.getElementsByTagName("actor");
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < nodeList.getLength(); i++) {
				sb.append(nodeList.item(i).getTextContent());
			}
			String actorsInfo = sb.toString();

			try {
				// removing title and actors info
				Element element = (Element) doc.getElementsByTagName("actors").item(0);
				element.getParentNode().removeChild(element);
				element = (Element) doc.getElementsByTagName("title").item(0);
				element.getParentNode().removeChild(element);
			} catch (NullPointerException e) {
				// file doesn't have actors/title
			}

			doc.normalize();
			String rest = doc.getElementsByTagName("movie").item(0).getTextContent();

			float gamma3 = 1 - gamma[0] - gamma[1];
			if (gamma3 < 0) {
				LOGGER.log(Level.SEVERE, "gamma3 is less than 0");
				gamma3 = 0;
			}
			Document lDoc = new Document();
			lDoc.add(new StringField(GeneralIndexer.DOCNAME_ATTRIB, FilenameUtils.removeExtension(file.getName()),
					Field.Store.YES));
			TextField titleField = new TextField(GeneralIndexer.TITLE_ATTRIB, title, Field.Store.YES);
			TextField actorsField = new TextField(ACTORS_ATTRIB, actorsInfo, Field.Store.YES);
			TextField contentField = new TextField(GeneralIndexer.CONTENT_ATTRIB, rest, Field.Store.YES);
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

	protected void indexXmlFile2(File file, IndexWriter writer, float smoothed, float gamma) {
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
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}
			String textContent = fileContent.replaceAll("<[^>]*>", "").trim();
			Document doc = new Document();
			doc.add(new StringField(GeneralIndexer.DOCNAME_ATTRIB, FilenameUtils.removeExtension(file.getName()),
					Field.Store.YES));
			TextField titleField = new TextField(GeneralIndexer.TITLE_ATTRIB, title, Field.Store.YES);
			TextField contentField = new TextField(GeneralIndexer.CONTENT_ATTRIB, textContent, Field.Store.YES);
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
