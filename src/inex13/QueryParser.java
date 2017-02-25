package inex13;

import inex09.InexQuery;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class QueryParser {

	public static void main(String[] args) {
		buildQueries("data/queries/inex_ld/2013-ld-adhoc-topics.xml",
				"data/queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels");
	}

	public static HashMap<Integer, InexQuery> buildQueries(String path, String qrelPath) {
		HashMap<Integer, InexQuery> map = new HashMap<Integer, InexQuery>();
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setNamespaceAware(true);
		dbf.setValidating(true);
		DocumentBuilder db;
		Document doc;
		try {
			db = dbf.newDocumentBuilder();
			doc = db.parse(path);
			NodeList nodeList = doc.getElementsByTagName("topic");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				int id = Integer.parseInt(node.getAttributes()
						.getNamedItem("id").getNodeValue());
				String queryText = getText(findSubNode("title", node));
				map.put(id, new InexQuery(id, queryText));
			}
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// adding relevant answers to the query
		File qrelFile = new File(qrelPath);
		Scanner sc = null;
		try {
			sc = new Scanner(qrelFile);
			String line;
			while (sc.hasNextLine()) {
				line = sc.nextLine();
				Pattern ptr = Pattern.compile("(\\d+)	Q0	(\\d+)	(1|0)");
				Matcher m = ptr.matcher(line);
				if (m.find()){
					if (m.group(3).equals("1")){
						int id = Integer.parseInt(m.group(1));
						String rel = m.group(2);
						InexQuery query = map.get(id);
						query.relDocs.add(rel);
					}
				} else {
					System.out.println("regex failed!!!");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		sc.close();
		return map;
	}

	static Node findSubNode(String name, Node node) {
		if (node.getNodeType() != Node.ELEMENT_NODE) {
			System.err.println("Error: Search node not of element type");
			System.exit(22);
		}

		if (!node.hasChildNodes())
			return null;

		NodeList list = node.getChildNodes();
		for (int i = 0; i < list.getLength(); i++) {
			Node subnode = list.item(i);
			if (subnode.getNodeType() == Node.ELEMENT_NODE) {
				if (subnode.getNodeName().equals(name))
					return subnode;
			}
		}
		return null;
	}

	static String getText(Node node) {
		StringBuffer result = new StringBuffer();
		if (!node.hasChildNodes())
			return "";

		NodeList list = node.getChildNodes();
		for (int i = 0; i < list.getLength(); i++) {
			Node subnode = list.item(i);
			if (subnode.getNodeType() == Node.TEXT_NODE) {
				result.append(subnode.getNodeValue());
			} else if (subnode.getNodeType() == Node.CDATA_SECTION_NODE) {
				result.append(subnode.getNodeValue());
			} else if (subnode.getNodeType() == Node.ENTITY_REFERENCE_NODE) {
				// Recurse into the subtree for text
				// (and ignore comments)
				result.append(getText(subnode));
			}
		}

		return result.toString();
	}

}
