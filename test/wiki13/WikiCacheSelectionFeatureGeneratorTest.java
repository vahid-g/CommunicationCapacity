package wiki13;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.junit.BeforeClass;
import org.junit.Test;

public class WikiCacheSelectionFeatureGeneratorTest {

	private static final double epsilon = 0.01;

	private static RAMDirectory ramDirectory;

	private static WikiCacheSelectionFeatureGenerator wcse = new WikiCacheSelectionFeatureGenerator();

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
	}

	@Test
	public void testCoveredQueryTermRatio() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			String[] queryTokens = { "shit", "six", "weeks" };
			double f1Ratio = wcse.coveredQueryTermRatio(reader, queryTokens, "f1");
			double f2Ratio = wcse.coveredQueryTermRatio(reader, queryTokens, "f2");
			assertEquals(0.33, f1Ratio, epsilon);
			assertEquals(0.66, f2Ratio, epsilon);
		}
	}

	@Test
	public void testMeanNormalizedDocumentFrequency() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			String[] queryTokens = { "shit", "six" };
			double result = wcse.meanNormalizedDocumentFrequency(reader, queryTokens, "f1");
			assertEquals(0.5, result, epsilon);
		}
	}

	@Test
	public void testMinNormalizedDocumentFrequency() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			String[] queryTokens = { "shit", "six" };
			double result = wcse.minNormalizedDocumentFrequency(reader, queryTokens, "f1");
			assertEquals(0.0, result, epsilon);
		}
	}

	@Test
	public void testCoveredBiwordRatio() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			double result = wcse.coveredBiwordRatio(new IndexSearcher(reader), "six weeks time the mess you", "f2");
			assertEquals(0.5, result, epsilon);
		}
	}

	@Test
	public void testMeanNormalizedDocumentBiwordFrequency() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			double result = wcse.meanNormalizedDocumentBiwordFrequency(new IndexSearcher(reader), "weeks time", "f2");
			assertEquals(1, result, epsilon);
		}
	}

	@Test
	public void testMinNormalizedDocumentBiwordFrequency() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			double result = wcse.minNormalizedDocumentBiwordFrequency(new IndexSearcher(reader), "weeks time", "f2");
			assertEquals(1, result, epsilon);
		}
	}

	@Test
	public void testTermPopularityFeaturesAll() throws IOException {
		try (IndexReader reader = DirectoryReader.open(ramDirectory)) {
			List<Double> result = wcse.termPopularityFeatures(reader, "weeks", "f2");
			assertEquals(10, result.get(0), epsilon);
			assertEquals(10, result.get(1), epsilon);
			assertEquals(10, result.get(2), epsilon);
			assertEquals(10, result.get(3), epsilon);
		}
	}

}
