package amazon.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.sun.org.apache.xerces.internal.dom.DocumentImpl;

import amazon.AmazonDeweyConverter;
import amazon.AmazonDocumentField;
import amazon.AmazonIsbnConverter;
import junit.framework.TestCase;

public class AmazonIndexerTest extends TestCase {

	AmazonIndexer indexer;

	public void setUp() {
		AmazonDocumentField[] fields = { AmazonDocumentField.TITLE, AmazonDocumentField.CONTENT,
				AmazonDocumentField.CREATORS, AmazonDocumentField.TAGS, AmazonDocumentField.DEWEY };
		AmazonIsbnConverter isbnConverter = AmazonIsbnConverter
				.getInstance("data/amazon_data/amazon-lt.isbn.thingID.csv");
		AmazonDeweyConverter deweyConverter = AmazonDeweyConverter.getInstance("data/amazon_data/dewey.csv");
		indexer = new AmazonIndexer(fields, isbnConverter, deweyConverter);
	}

	@Test
	public void testExtractNodeTextContent() {
		Document xmlDoc = new DocumentImpl();
		Element root = xmlDoc.createElement("book");
		Node innerItem = xmlDoc.createElement("innerItem");
		innerItem.appendChild(xmlDoc.createTextNode("innerText"));
		Node item = xmlDoc.createElement("outerItem");
		item.appendChild(innerItem);
		item.appendChild(xmlDoc.createTextNode("outerText"));
		root.appendChild(item);
		xmlDoc.appendChild(root);
		assertEquals("innerText outerText", indexer.extractNodeTextContent(root));
	}

	public void testExtractNodesTextFromXml() throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		File file = new File("data/test_data/1931243999.xml");
		org.w3c.dom.Document xmlDoc = db.parse(file);
		Node bookNode = xmlDoc.getElementsByTagName("book").item(0);
		assertEquals("Journey Around My Room (Green Integer)",
				indexer.extractNodesTextFromXml(xmlDoc, AmazonDocumentField.TITLE, bookNode));
		assertEquals("Mark Axelrod Translator Xavier de Maistre Author",
				indexer.extractNodesTextFromXml(xmlDoc, AmazonDocumentField.CREATORS, bookNode));
		assertEquals("unread literature Fiction",
				indexer.extractNodesTextFromXml(xmlDoc, AmazonDocumentField.TAGS, bookNode));
		assertEquals("910", indexer.extractNodesTextFromXml(xmlDoc, AmazonDocumentField.DEWEY, bookNode));
	}

	public void testParseAmazonXml() {
		File file = new File("data/test_data/1931243999.xml");
		Map<AmazonDocumentField, String> dMap = indexer.parseAmazonXml(file);
		assertEquals("Geography & travel", dMap.get(AmazonDocumentField.DEWEY));
		assertEquals("unread literature Fiction", dMap.get(AmazonDocumentField.TAGS));
	}

	public void testIndex() throws IOException, ParseException {
		File file = new File("data/test_data/1931243999.xml");
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
		indexWriterConfig.setOpenMode(OpenMode.CREATE);
		indexWriterConfig.setRAMBufferSizeMB(1024.00);
		indexWriterConfig.setSimilarity(new BM25Similarity());
		FSDirectory directory = FSDirectory.open(Paths.get("data/index"));
		IndexWriter writer = new IndexWriter(directory, indexWriterConfig);
		indexer.index(file, writer);
		writer.close();
		IndexReader reader = DirectoryReader.open(directory);
		IndexSearcher searcher = new IndexSearcher(reader);
		searcher.setSimilarity(new BM25Similarity());
		StandardAnalyzer analyzer = new StandardAnalyzer();
		QueryParser parser = new QueryParser("title", analyzer);
		Query query = parser.parse("Journey Around My Room");
		TopDocs topDocs = searcher.search(query, 10);
		org.apache.lucene.document.Document doc = searcher.doc(topDocs.scoreDocs[0].doc);
		directory.close();
		assertEquals("Journey Around My Room (Green Integer)", doc.get("title"));
		assertEquals("27570", doc.get("ltid"));
	}

}
