package amazon.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

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

import amazon.AmazonDocumentField;
import amazon.datatools.AmazonDeweyConverter;
import amazon.datatools.AmazonIsbnConverter;
import junit.framework.TestCase;

public class AmazonIndexerTest extends TestCase {

	AmazonIndexer indexer;

	public void setUp() {
		AmazonDocumentField[] fields = { AmazonDocumentField.TITLE, AmazonDocumentField.CONTENT,
				AmazonDocumentField.CREATORS, AmazonDocumentField.TAGS, AmazonDocumentField.DEWEY };
		Map<String, String> isbnConverter = AmazonIsbnConverter
				.loadIsbnToLtidMap("data/amazon_data/amazon-lt.isbn.thingID.csv");
		AmazonDeweyConverter deweyConverter = AmazonDeweyConverter.getInstance("data/amazon_data/dewey.csv");
		indexer = new AmazonIndexer(fields, isbnConverter, deweyConverter);
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
