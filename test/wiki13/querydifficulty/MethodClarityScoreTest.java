package wiki13.querydifficulty;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import indexing.InexFile;
import query.ExperimentQuery;
import wiki13.WikiFileIndexer;

public class MethodClarityScoreTest {

    @Test
    public void testComputeQueryDifficulty() throws IOException {
	InexFile inexFile = new InexFile("test_data/sample_wiki_file.txt", 1);
	inexFile.title = "hanhan";
	IndexWriterConfig indexWriterConfig = new IndexWriterConfig(
		new StandardAnalyzer());
	indexWriterConfig.setOpenMode(OpenMode.CREATE);
	try (RAMDirectory rd = new RAMDirectory()) {
	    try (IndexWriter writer = new IndexWriter(rd, indexWriterConfig)) {
		WikiFileIndexer indexer = new WikiFileIndexer();
		indexer.index(inexFile, writer);
	    }
	    IndexReader reader = DirectoryReader.open(rd);
	    List<ExperimentQuery> queries = new ArrayList<ExperimentQuery>();
	    ExperimentQuery query1 = new ExperimentQuery(1, "hanhan");
	    queries.add(query1);
	    ClarityScore mcs = new ClarityScore();
	    Map<String, Double> results = mcs.computeScore(reader, queries,
		    WikiFileIndexer.CONTENT_ATTRIB);
	    rd.close();
	    assertEquals(1 + Math.log(6 / 3), results.get("hanhan"), 0.01);
	}
    }
}
