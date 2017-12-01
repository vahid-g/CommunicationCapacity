package amazon.indexing;

import indexing.InexFile;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import junit.framework.TestCase;

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
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

import amazon.AmazonDocumentField;
import amazon.datatools.AmazonDeweyConverter;
import amazon.datatools.AmazonIsbnConverter;

public class AmazonIndexerTest extends TestCase {

    AmazonFileIndexer indexer;

    @Before
    public void setUp() {
	AmazonDocumentField[] fields = { AmazonDocumentField.TITLE,
		AmazonDocumentField.CONTENT, AmazonDocumentField.CREATORS,
		AmazonDocumentField.TAGS, AmazonDocumentField.DEWEY };
	Map<String, String> isbnConverter = AmazonIsbnConverter
		.loadIsbnToLtidMap("test_data/isbn_ltid.csv");
	AmazonDeweyConverter deweyConverter = AmazonDeweyConverter
		.getInstance("test_data/dewey.tsv");
	indexer = new AmazonFileIndexer(fields, isbnConverter, deweyConverter);
    }

    @Test
    public void testParseAmazonXml() {
	File file = new File("test_data/1931243999.xml");
	Map<AmazonDocumentField, String> dMap = indexer.parseAmazonXml(file);
	assertEquals("Geography & travel", dMap.get(AmazonDocumentField.DEWEY));
	assertEquals("unread literature Fiction",
		dMap.get(AmazonDocumentField.TAGS));
    }

    @Test
    public void testIndex() throws IOException, ParseException {
	InexFile inexFile = new InexFile("test_data/1931243999.xml", 1);
	IndexWriterConfig indexWriterConfig = new IndexWriterConfig(
		new StandardAnalyzer());
	indexWriterConfig.setOpenMode(OpenMode.CREATE);
	indexWriterConfig.setRAMBufferSizeMB(1024.00);
	indexWriterConfig.setSimilarity(new BM25Similarity());
	RAMDirectory rd = new RAMDirectory();
	IndexWriter writer = new IndexWriter(rd, indexWriterConfig);
	indexer.index(inexFile, writer);
	writer.close();
	IndexReader reader = DirectoryReader.open(rd);
	IndexSearcher searcher = new IndexSearcher(reader);
	searcher.setSimilarity(new BM25Similarity());
	StandardAnalyzer analyzer = new StandardAnalyzer();
	QueryParser parser = new QueryParser("title", analyzer);
	Query query = parser.parse("Journey Around My Room");
	TopDocs topDocs = searcher.search(query, 10);
	org.apache.lucene.document.Document doc = searcher
		.doc(topDocs.scoreDocs[0].doc);
	rd.close();
	assertEquals("Journey Around My Room (Green Integer)", doc.get("title"));
	assertEquals("27570", doc.get("ltid"));
    }

}
