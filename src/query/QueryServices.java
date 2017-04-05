package query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import wiki_inex09.WikiIndexer;

public class QueryServices {

	final static int TOP_DOC_COUNT = 100;

	static final Logger LOGGER = Logger
			.getLogger(QueryServices.class.getName());

	public static List<QueryResult> runQueries(List<ExperimentQuery> queries,
			String indexPath) {
		List<QueryResult> iqrList = new ArrayList<QueryResult>();
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexPath)))) {
			LOGGER.log(Level.INFO,
					"Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			for (ExperimentQuery queryDAO : queries) {
				// LOGGER.log(Level.INFO,queryCoutner++);
				Query query = buildLuceneQuery(queryDAO.text,
						WikiIndexer.TITLE_ATTRIB, WikiIndexer.CONTENT_ATTRIB);
				TopDocs topDocs = searcher.search(query, TOP_DOC_COUNT);
				QueryResult iqr = new QueryResult(queryDAO);
				for (int i = 0; i < Math.min(TOP_DOC_COUNT,
						topDocs.scoreDocs.length); i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					String docID = doc.get(WikiIndexer.DOCNAME_ATTRIB);
					String docTitle = doc.get(WikiIndexer.TITLE_ATTRIB);
					iqr.topResults.add(docID);
					iqr.topResultsTitle.add(docID + ": " + docTitle);
				}
				iqrList.add(iqr);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return iqrList;
	}

	public static Query buildLuceneQuery(String queryString, String... fields) {
		MultiFieldQueryParser parser = new MultiFieldQueryParser(fields,
				new StandardAnalyzer());
		parser.setDefaultOperator(Operator.OR);
		Query query = null;
		try {
			query = parser.parse(QueryParser.escape(queryString));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return query;
	}

	public static List<ExperimentQuery> loadMsnQueries(String queryFile,
			String qrelFile) {
		Map<String, List<String>> qidQrelMap = new HashMap<String, List<String>>();
		try (BufferedReader br = new BufferedReader(new FileReader(qrelFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				String qid = line.split(" ")[0];
				String qrel = line.split(" ")[2];
				if (qidQrelMap.containsKey(qid)) {
					List<String> rels = qidQrelMap.get(qid);
					rels.add(qrel);
				} else {
					List<String> rels = new ArrayList<String>();
					rels.add(qrel);
					qidQrelMap.put(qid, rels);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<ExperimentQuery> queryList = new ArrayList<ExperimentQuery>();
		try (BufferedReader br = new BufferedReader(new FileReader(queryFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				int index = line.lastIndexOf(" ");
				String text = line.substring(0, index).replace(",", "")
						.replace("\"", "");
				String qid = line.substring(index + 1);
				if (qidQrelMap.containsKey(qid)) {
					ExperimentQuery query = new ExperimentQuery(
							Integer.parseInt(qid), text);
					List<String> qrels = qidQrelMap.get(qid);
					query.setQrels(qrels);
					queryList.add(query);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return queryList;
	}

	public static List<ExperimentQuery> loadInexQueries(String path,
			String qrelPath) {
		// building qid -> qrels map
		HashMap<Integer, List<String>> qidQrels = new HashMap<Integer, List<String>>();
		try (Scanner sc = new Scanner(qrelPath)) {
			String line;
			while (sc.hasNextLine()) {
				line = sc.nextLine();
				Pattern ptr = Pattern.compile("(\\d+)	Q0	(\\d+)	(1|0)");
				Matcher m = ptr.matcher(line);
				if (m.find()) {
					if (m.group(3).equals("1")) { // sanity check that this is
													// relevant answer
						Integer qid = Integer.parseInt(m.group(1));
						String rel = m.group(2);
						List<String> qrels;
						if (qidQrels.containsKey(qid)) {
							qrels = qidQrels.get(qid);
						} else {
							qrels = new ArrayList<String>();
							qidQrels.put(qid, qrels);
						}
						qrels.add(rel);
					}
				} else {
					System.out.println("regex failed!!!");
				}
			}
		}

		// loading queries
		List<ExperimentQuery> queryList = new ArrayList<ExperimentQuery>();
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document doc = db.parse(new File(path));
			NodeList nodeList = doc.getElementsByTagName("topic");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				int qid = Integer.parseInt(node.getAttributes()
						.getNamedItem("id").getNodeValue());
				String queryText = getText(findSubNode("title", node));
				List<String> qrels = qidQrels.get(qid);
				ExperimentQuery iq = new ExperimentQuery(qid, queryText, qrels);
				queryList.add(iq);
			}
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return queryList;
	}

	public static HashMap<Integer, ExperimentQuery> buildQueries(String path,
			String qrelPath) {
		HashMap<Integer, ExperimentQuery> qidQueryMap = new HashMap<Integer, ExperimentQuery>();
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setNamespaceAware(true);
		dbf.setValidating(true);
		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			org.w3c.dom.Document doc = db.parse(new File(path));
			NodeList nodeList = doc.getElementsByTagName("topic");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				int id = Integer.parseInt(node.getAttributes()
						.getNamedItem("id").getNodeValue());
				String queryText = getText(findSubNode("title", node));
				ExperimentQuery iq = new ExperimentQuery(id, queryText);
				qidQueryMap.put(id, iq);
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
				if (m.find()) {
					if (m.group(3).equals("1")) {
						int id = Integer.parseInt(m.group(1));
						String rel = m.group(2);
						ExperimentQuery query = qidQueryMap.get(id);
						query.qrels.add(rel);
					}
				} else {
					System.out.println("regex failed!!!");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		sc.close();
		return qidQueryMap;
	}

	public static Node findSubNode(String name, Node node) {
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
