package wiki13.cache_selection;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.RAMDirectory;
import org.junit.BeforeClass;
import org.junit.Test;

import indexing.BiwordAnalyzer;
import wiki13.WikiFileIndexer;

public class WikiCacheSelectionFeatureGeneratorTest {

	private static final double epsilon = 0.01;

	private static RAMDirectory ramDirectory;

	private static RAMDirectory biwordRamDirectory;

	private static RunFeatureExtraction wcse = new RunFeatureExtraction();

	private static Analyzer analyzer = new StandardAnalyzer();

	private static Analyzer biwordAnalyzer = new BiwordAnalyzer();

	@BeforeClass
	public static void beforeClass() throws IOException {
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
		indexWriterConfig.setOpenMode(OpenMode.CREATE);
		ramDirectory = new RAMDirectory();
		try (IndexWriter writer = new IndexWriter(ramDirectory, indexWriterConfig)) {
			Document doc = new Document();
			doc.add(new TextField("f1", "this is the new shit", Store.NO));
			doc.add(new TextField("f2", "six feet down, in six weeks time", Store.NO));
			doc.add(new StoredField(WikiFileIndexer.WEIGHT_ATTRIB, 10));
			writer.addDocument(doc);
		}
		indexWriterConfig = new IndexWriterConfig(new BiwordAnalyzer());
		biwordRamDirectory = new RAMDirectory();
		try (IndexWriter writer = new IndexWriter(biwordRamDirectory, indexWriterConfig)) {
			Document doc = new Document();
			doc.add(new TextField("f1", "this is the new shit", Store.NO));
			doc.add(new TextField("f2", "six feet down, in six weeks time", Store.NO));
			doc.add(new StoredField(WikiFileIndexer.WEIGHT_ATTRIB, 10));
			writer.addDocument(doc);
		}
	}

	@Test
	public void testCoveredTokenRatio() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			String queryText = "shit six weeks";
			double f1Ratio = wcse.coveredTokenRatio(reader, queryText, "f1", analyzer);
			double f2Ratio = wcse.coveredTokenRatio(reader, queryText, "f2", analyzer);
			assertEquals(0.33, f1Ratio, epsilon);
			assertEquals(0.66, f2Ratio, epsilon);
		}
		try (IndexReader reader = DirectoryReader.open(biwordRamDirectory)) {
			double result = wcse.coveredTokenRatio(reader, "six weeks time the mess in you", "f2",
					new BiwordAnalyzer());
			assertEquals(0.33, result, epsilon);
		}
	}

	@Test
	public void testMeanNormalizedTokenDocumentFrequency() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			double result = wcse.meanNormalizedTokenDocumentFrequency(reader, "shit six", "f1", analyzer);
			assertEquals(0.5, result, epsilon);
		}
	}

	@Test
	public void testMeanNormalizedTokenDocumentFrequency2() throws IOException {
		try (IndexReader reader = DirectoryReader.open(biwordRamDirectory)) {
			double result = wcse.meanNormalizedTokenDocumentFrequency(reader, "weeks time", "f2", biwordAnalyzer);
			assertEquals(1, result, epsilon);
		}
	}

	@Test
	public void testMinNormalizedTokenDocumentFrequency() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			double result = wcse.minNormalizedTokenDocumentFrequency(reader, "shit six", "f1", analyzer);
			assertEquals(0.0, result, epsilon);
		}
	}

	@Test
	public void testMinNormalizedTokenDocumentFrequency2() throws IOException {
		try (IndexReader reader = DirectoryReader.open(biwordRamDirectory)) {
			double result = wcse.minNormalizedTokenDocumentFrequency(reader, "weeks time", "f2", biwordAnalyzer);
			assertEquals(1, result, epsilon);
		}
	}

	@Test
	public void testTokenPopularityFeatures() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			List<Double> result = wcse.tokenPopularityFeatures(reader, "weeks", "f2", analyzer);
			assertEquals(10, result.get(0), epsilon);
			assertEquals(10, result.get(1), epsilon);
			assertEquals(10, result.get(2), epsilon);
			assertEquals(10, result.get(3), epsilon);
		}
	}

}
