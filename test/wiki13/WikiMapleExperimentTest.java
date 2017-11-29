package wiki13;

import static org.junit.Assert.assertEquals;
import indexing.InexFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

import query.ExperimentQuery;
import query.QueryResult;

public class WikiMapleExperimentTest {

	@Test
	public void testFilterQueryResult() {
		QueryResult result1 = new QueryResult(new ExperimentQuery(1,
				"query text"));
		result1.addResult("doc1", "good doc");
		result1.addResult("doc2", "fairDoc");
		result1.addResult("doc3", "bad doc");
		Map<String, Double> idPopMap = new HashMap<String, Double>();
		idPopMap.put("doc1", 1.0);
		idPopMap.put("doc2", 10.0);
		idPopMap.put("doc3", 100.0);
		assertEquals(3, result1.getTopDocuments().size());
		QueryResult result2 = WikiMapleExperiment.filterQueryResult(result1,
				idPopMap, 1);
		assertEquals(3, result2.getTopDocuments().size());
		result2 = WikiMapleExperiment.filterQueryResult(result1, idPopMap, 11);
		assertEquals(3, result1.getTopDocuments().size());
		assertEquals(1, result2.getTopDocuments().size());
	}
	
	@Test
	public void testComputeQueryDifficulty() throws IOException {
		InexFile inexFile = new InexFile("test_data/sample_wiki_file.txt", 1);
		inexFile.title = "hanhan";
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
		indexWriterConfig.setOpenMode(OpenMode.CREATE);
		RAMDirectory rd = new RAMDirectory();
		IndexWriter writer = new IndexWriter(rd, indexWriterConfig);
		WikiFileIndexer indexer = new WikiFileIndexer();
		indexer.index(inexFile, writer);
		writer.close();
		IndexReader reader = DirectoryReader.open(rd);
		List<ExperimentQuery> queries = new ArrayList<ExperimentQuery>();
		ExperimentQuery query1 = new ExperimentQuery(1, "hanhan");
		queries.add(query1);
		List<Double> results = WikiMapleExperiment.computeQueryDifficulty(reader, queries, WikiFileIndexer.TITLE_ATTRIB);
		rd.close();
		assertEquals(1 + Math.log(6/3), results.get(0), 0.01);
	}

}
