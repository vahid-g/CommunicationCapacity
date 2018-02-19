package amazon.query;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import amazon.AmazonDocumentField;

public class AmazonQueryGenerator {

	static final Logger LOGGER = Logger.getLogger(AmazonQueryGenerator.class.getName());

	public static void main(String[] args) {
		File datasetDirectory = new File("???");
		String[] extensions = { ".xml" };
		Collection<File> dataset = FileUtils.listFiles(datasetDirectory, extensions, true);
		System.out.println(dataset.size());
	}

	protected Document buildXmlDocument() {
		Document doc = null;
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder;
			docBuilder = docFactory.newDocumentBuilder();

			// root elements
			doc = docBuilder.newDocument();
			Element rootElement = doc.createElement("topics");
			doc.appendChild(rootElement);

			// staff elements
			Element staff = doc.createElement("Staff");
			rootElement.appendChild(staff);

			// set attribute to staff element
			Attr attr = doc.createAttribute("id");
			attr.setValue("1");
			staff.setAttributeNode(attr);

			// shorten way
			// staff.setAttribute("id", "1");

			// firstname elements
			Element firstname = doc.createElement("firstname");
			firstname.appendChild(doc.createTextNode("yong"));
			staff.appendChild(firstname);

			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File("C:\\file.xml"));

			// Output to console for testing
			// StreamResult result = new StreamResult(System.out);

			transformer.transform(source, result);

			System.out.println("File saved!");
		} catch (ParserConfigurationException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (TransformerConfigurationException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (TransformerException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return doc;
	}

	protected String buildKeywordQuery(Map<AmazonDocumentField, String> xmlFieldsMap) {
		StringBuilder keywordQueryBuilder = new StringBuilder();
		String creators = xmlFieldsMap.get(AmazonDocumentField.CREATORS);
		String title = xmlFieldsMap.get(AmazonDocumentField.TITLE);
		keywordQueryBuilder.append(creators);
		keywordQueryBuilder.append(title);
		return keywordQueryBuilder.toString();
	}

	protected Element buildTopicElement(Document doc, String id, String mediatedQuery) {
		Element topicElement = doc.createElement("topic");
		Attr topicIdAttribute = doc.createAttribute("id");
		topicIdAttribute.setValue(id);
		topicElement.setAttributeNode(topicIdAttribute);
		Element mediatedQueryElement = doc.createElement("mediated_query");
		mediatedQueryElement.appendChild(doc.createTextNode(mediatedQuery));
		topicElement.appendChild(mediatedQueryElement);
		return topicElement;
	}
}
