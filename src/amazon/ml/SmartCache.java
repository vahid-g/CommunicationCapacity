package amazon.ml;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.NodeList;

public class SmartCache {

	private static final Logger LOGGER = Logger.getLogger(SmartCache.class.getName());
	
	//TODO: given a list of paths with rates and labels, build the complete feature vector.
	// All the vectors will have a label.

	public List<String> extractFeatureVector(File amazonXmlFile) {
		List<String> features = new ArrayList<String>();
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document xmlDoc = db.parse(amazonXmlFile);
			features.add(getItemOrZero(xmlDoc.getElementsByTagName("binding")));
			features.add(getItemOrZero(xmlDoc.getElementsByTagName("listprice")).replace("$", ""));
			String date = getItemOrZero(xmlDoc.getElementsByTagName("publicationdate"));
			if (date.contains("-"))
				date = date.substring(0, date.indexOf('-'));
			features.add(date);
			String dewey = getItemOrZero(xmlDoc.getElementsByTagName("dewey"));
			if (dewey.contains("."))
				dewey.substring(0, dewey.indexOf('.'));
			features.add(dewey);
			features.add(getItemOrZero(xmlDoc.getElementsByTagName("numberofpages")));

			features.add(Integer.toString(xmlDoc.getElementsByTagName("image").getLength()));
			features.add(Integer.toString(xmlDoc.getElementsByTagName("review").getLength()));
			features.add(Integer.toString(xmlDoc.getElementsByTagName("editorialreview").getLength()));
			features.add(Integer.toString(xmlDoc.getElementsByTagName("creator").getLength()));
			features.add(Integer.toString(xmlDoc.getElementsByTagName("similarproduct").getLength()));
			features.add(Integer.toString(xmlDoc.getElementsByTagName("browseNode").getLength()));

			// last review data
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return features;
	}

	private String getItemOrZero(NodeList nodelist) {
		if (nodelist.getLength() == 0)
			return "0";
		else
			return nodelist.item(0).getTextContent();
	}
}
