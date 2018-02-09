package query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import indexing.GeneralIndexer;

public class QueryServices {

    static final Logger LOGGER = Logger
	    .getLogger(QueryServices.class.getName());

    public static void main(String[] args) {
	loadInexQueries("inex14sbs.topics.xml", "inex14sbs.qrels", "title");
    }

    public static List<QueryResult> runQueries(List<ExperimentQuery> queries,
	    String indexPath) {
	String[] attribs = { GeneralIndexer.TITLE_ATTRIB,
		GeneralIndexer.CONTENT_ATTRIB };
	return runQueries(queries, indexPath, attribs, 200);
    }

    public static List<QueryResult> runQueries(List<ExperimentQuery> queries,
	    String indexPath, String[] attribs, int topDocCount) {
	return runQueries(queries, indexPath, new ClassicSimilarity(), attribs,
		topDocCount);
    }

    public static List<QueryResult> runQueries(List<ExperimentQuery> queries,
	    String indexPath, Similarity similarity, String[] attribs,
	    int topDocCount) {
	List<QueryResult> iqrList = new ArrayList<QueryResult>();
	try (IndexReader reader = DirectoryReader
		.open(FSDirectory.open(Paths.get(indexPath)))) {
	    LOGGER.log(Level.INFO,
		    "Number of docs in index: " + reader.numDocs());
	    IndexSearcher searcher = new IndexSearcher(reader);
	    searcher.setSimilarity(similarity);
	    Map<String, Float> fieldBoostMap = new HashMap<String, Float>();
	    for (String attrib : attribs) {
		fieldBoostMap.put(attrib, 1.0f);
	    }
	    LuceneQueryBuilder lqb = new LuceneQueryBuilder(fieldBoostMap);
	    for (ExperimentQuery queryDAO : queries) {
		Query query = lqb.buildQuery(queryDAO.getText());
		TopDocs topDocs = searcher.search(query, topDocCount);
		QueryResult iqr = new QueryResult(queryDAO);
		for (int i = 0; i < Math.min(topDocCount,
			topDocs.scoreDocs.length); i++) {
		    Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
		    String docId = doc.get(GeneralIndexer.DOCNAME_ATTRIB);
		    String docTitle = doc.get(GeneralIndexer.TITLE_ATTRIB);
		    iqr.addResult(docId, docTitle);
		}
		iqrList.add(iqr);
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return iqrList;
    }

    public static List<QueryResult> runQueriesWithBoosting(
	    List<ExperimentQuery> queries, String indexPath,
	    Similarity similarity, Map<String, Float> fieldBoostMap,
	    int topDocCount) {
	return runQueriesWithBoosting(queries, indexPath, similarity,
		new LuceneQueryBuilder(fieldBoostMap), false, topDocCount);
    }

    public static List<QueryResult> runQueriesWithBoosting(
	    List<ExperimentQuery> queries, String indexPath,
	    Similarity similarity, LuceneQueryBuilder lqb, boolean explain,
	    int topDocCount) {
	List<QueryResult> iqrList = new ArrayList<QueryResult>();
	try (IndexReader reader = DirectoryReader
		.open(FSDirectory.open(Paths.get(indexPath)))) {
	    LOGGER.log(Level.INFO,
		    "Number of docs in index: " + reader.numDocs());
	    IndexSearcher searcher = new IndexSearcher(reader);
	    searcher.setSimilarity(similarity);
	    for (ExperimentQuery experimentQuery : queries) {
		Query query = lqb.buildQuery(experimentQuery.getText());
		ScoreDoc[] hits = searcher.search(query, topDocCount).scoreDocs;
		QueryResult iqr = new QueryResult(experimentQuery);
		for (int i = 0; i < Math.min(topDocCount, hits.length); i++) {
		    Document doc = searcher.doc(hits[i].doc);
		    String docId = doc.get(GeneralIndexer.DOCNAME_ATTRIB);
		    String docTitle = doc.get(GeneralIndexer.TITLE_ATTRIB);
		    if (explain) {
			iqr.addResult(docId, docTitle,
				hits[i].score + " idf: " + buildIdfString(
					searcher.explain(query, hits[i].doc)));
		    } else {
			iqr.addResult(docId, docTitle);
		    }
		}
		iqrList.add(iqr);
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return iqrList;
    }

    private static String buildIdfString(Explanation explanation) {
	LinkedList<Explanation> queue = new LinkedList<Explanation>();
	queue.add(explanation);
	StringBuilder sb = new StringBuilder();
	while (!queue.isEmpty()) {
	    Explanation exp = queue.removeLast();
	    for (Explanation child : exp.getDetails())
		queue.add(child);
	    if (exp.getDescription().startsWith("idf"))
		sb.append(exp.getValue() + " ");
	}
	return sb.toString();
    }

    public static int lookupDocumentId(String id, IndexReader reader) {
	TermQuery termQuery = new TermQuery(
		new Term(GeneralIndexer.DOCNAME_ATTRIB, id));
	IndexSearcher searcher = new IndexSearcher(reader);
	TopDocs topDocs = null;
	try {
	    topDocs = searcher.search(termQuery, 10);
	} catch (IOException e) {
	    LOGGER.log(Level.SEVERE, "lookup failed", e);
	}
	if (topDocs != null)
	    return topDocs.totalHits;
	else
	    return 0;
    }

    public static List<ExperimentQuery> loadMsnQueries(String queryPath,
	    String qrelPath) {
	List<ExperimentQuery> queryList = new ArrayList<ExperimentQuery>();
	try (FileInputStream fis = new FileInputStream(qrelPath);
		BufferedReader br = new BufferedReader(
			new FileReader(queryPath))) {
	    Map<Integer, Set<Qrel>> qidQrelMap = Qrel.loadQrelFile(fis);
	    String line;
	    while ((line = br.readLine()) != null) {
		int index = line.lastIndexOf(" ");
		String text = line.substring(0, index).replace(",", "")
			.replace("\"", "");
		Integer qid = Integer.parseInt(line.substring(index + 1));
		if (qidQrelMap.containsKey(qid)) {
		    Set<Qrel> qrels = qidQrelMap.get(qid);
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
	} catch (IOException e) {
	    LOGGER.log(Level.SEVERE, e.getMessage(), e);
	}
	return queryList;
    }

    public static List<ExperimentQuery> loadInexQueries(String path,
	    String qrelPath) {
	return loadInexQueries(path, qrelPath, "title");
    }

    public static List<ExperimentQuery> loadInexQueries(String path,
	    String qrelPath, String... queryLabels) {
	List<ExperimentQuery> queryList = new ArrayList<ExperimentQuery>();
	try (FileInputStream fis = new FileInputStream(qrelPath)) {
	    Map<Integer, Set<Qrel>> qidQrels = Qrel.loadQrelFile(fis);
	    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	    DocumentBuilder db = dbf.newDocumentBuilder();
	    org.w3c.dom.Document doc = db.parse(new File(path));
	    NodeList nodeList = doc.getElementsByTagName("topic");
	    int nullQrelCounter = 0;
	    for (int i = 0; i < nodeList.getLength(); i++) {
		Node node = nodeList.item(i);
		int qid = Integer.parseInt(
			node.getAttributes().getNamedItem("id").getNodeValue());
		StringBuilder sb = new StringBuilder();
		for (String queryLabel : queryLabels) {
		    String queryText = getText(findSubNode(queryLabel, node))
			    .trim();
		    sb.append(queryText + " ");
		}
		String queryText = sb.toString().trim();
		if (queryText.equals("")) {
		    LOGGER.log(Level.SEVERE,
			    "query: " + qid + " has empty aggregated text");
		    continue;
		}
		Set<Qrel> qrels = qidQrels.get(qid);
		if (qrels == null) {
		    nullQrelCounter++;
		} else {
		    ExperimentQuery iq = new ExperimentQuery(qid, queryText,
			    qrels);
		    queryList.add(iq);
		}
	    }
	    LOGGER.log(Level.WARNING, "Number of queries without qrel in "
		    + qrelPath + " is: " + nullQrelCounter);
	} catch (Exception e) {
	    LOGGER.log(Level.SEVERE, e.getMessage(), e);
	}
	return queryList;
    }

    private static Node findSubNode(String name, Node node) {
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
