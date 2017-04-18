package query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
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

import wiki_inex09.Wiki09Indexer;

public class QueryServices {

	final static int TOP_DOC_COUNT = 100;

	static final Logger LOGGER = Logger
			.getLogger(QueryServices.class.getName());

	public static void main(String[] args) {
		String queryFile = "data/queries/inex_ld/2013-ld-adhoc-topics.xml";
		String qrelsFile = "data/queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
		loadInexQueries(queryFile, qrelsFile);
	}

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
						Wiki09Indexer.TITLE_ATTRIB, Wiki09Indexer.CONTENT_ATTRIB);
				TopDocs topDocs = searcher.search(query, TOP_DOC_COUNT);
				QueryResult iqr = new QueryResult(queryDAO);
				for (int i = 0; i < Math.min(TOP_DOC_COUNT,
						topDocs.scoreDocs.length); i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					String docID = doc.get(Wiki09Indexer.DOCNAME_ATTRIB);
					String docTitle = doc.get(Wiki09Indexer.TITLE_ATTRIB);
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

	public static List<ExperimentQuery> loadMsnQueries(String queryPath,
			String qrelPath) {
		Map<Integer, Set<String>> qidQrelMap = loadQrelFile(qrelPath);
		List<ExperimentQuery> queryList = new ArrayList<ExperimentQuery>();
		try (BufferedReader br = new BufferedReader(new FileReader(queryPath))) {
			String line;
			while ((line = br.readLine()) != null) {
				int index = line.lastIndexOf(" ");
				String text = line.substring(0, index).replace(",", "")
						.replace("\"", "");
				Integer qid = Integer.parseInt(line.substring(index + 1));
				if (qidQrelMap.containsKey(qid)) {
					Set<String> qrels = qidQrelMap.get(qid);
					if (qrels == null) {
						LOGGER.log(Level.SEVERE, "no qrels for query: " + qid
								+ ":" + text + "in file: " + qrelPath);
					} else {
						ExperimentQuery iq = new ExperimentQuery(qid, text,
								qrels);
						queryList.add(iq);
					}
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
		HashMap<Integer, Set<String>> qidQrels = loadQrelFile(qrelPath);

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
				Set<String> qrels = qidQrels.get(qid);
				if (qrels == null) {
					LOGGER.log(Level.SEVERE, "no qrels for query: " + qid + ":"
							+ queryText + "in file: " + qrelPath);
				} else {
					ExperimentQuery iq = new ExperimentQuery(qid, queryText,
							qrels);
					queryList.add(iq);
				}
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

	private static HashMap<Integer, Set<String>> loadQrelFile(String path) {
		HashMap<Integer, Set<String>> qidQrels = new HashMap<Integer, Set<String>>();
		try (Scanner sc = new Scanner(new File(path))) {
			String line;
			while (sc.hasNextLine()) {
				line = sc.nextLine();
				Pattern ptr = Pattern.compile("(\\d+)\\sQ0\\s(\\d+)\\s(1|0)");
				Matcher m = ptr.matcher(line);
				if (m.find()) {
					if (m.group(3).equals("1")) {
						Integer qid = Integer.parseInt(m.group(1));
						String rel = m.group(2);
						Set<String> qrels = qidQrels.get(qid);
						if (qrels == null) {
							qrels = new HashSet<String>();
							qrels.add(rel);
							qidQrels.put(qid, qrels);
						} else {
							qrels.add(rel);
						}
					}
				} else {
					LOGGER.log(Level.WARNING, "regex failed for line: " + line);
				}
			}
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, "QREL file not found!");
		}
		return qidQrels;
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

	private static String getText(Node node) {
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
