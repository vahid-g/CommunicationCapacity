package wiki13.querydifficulty;

import static org.junit.Assert.assertEquals;
import indexing.InexFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import query.ExperimentQuery;
import wiki13.WikiFileIndexer;
import wiki13.cache_selection.JelinekMercerScore;

public class JelinekMercerScoreTest {

    @Test
    public void test() throws IOException {
   	InexFile inexFile = new InexFile("test_data/sample_wiki_file.txt", 1);
   	inexFile.title = "hanhan";
   	InexFile inexFile2 = new InexFile("test_data/sample_global_file.txt", 1);
   	try (RAMDirectory rd = new RAMDirectory(); RAMDirectory rd2 = new RAMDirectory()) {
   	    try (IndexWriter writer = new IndexWriter(rd, new IndexWriterConfig()); 
   		    IndexWriter writer2 = new IndexWriter(rd2, new IndexWriterConfig())) {
   		WikiFileIndexer indexer = new WikiFileIndexer();
   		indexer.index(inexFile, writer);
   		indexer.index(inexFile2, writer2);
   	    }
   	    IndexReader reader = DirectoryReader.open(rd);
   	    List<ExperimentQuery> queries = new ArrayList<ExperimentQuery>();
   	    ExperimentQuery query1 = new ExperimentQuery(1, "hanhan");
   	    queries.add(query1);
   	    JelinekMercerScore jms = new JelinekMercerScore(DirectoryReader.open(rd2));
   	    Map<String, Double> results = jms.computeScore(reader, queries,
   		    WikiFileIndexer.CONTENT_ATTRIB);
   	    rd.close();
   	    rd2.close();
   	    double score = 0.5 * (2.0 / 6) + 0.5 * (3.0 / 8);
   	    assertEquals(score, results.get("hanhan"), 0.01);
   	}
       }

}
