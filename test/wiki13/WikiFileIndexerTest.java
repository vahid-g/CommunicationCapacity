package wiki13;

import static org.junit.Assert.*;

import java.io.IOException;

import indexing.InexFile;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

public class WikiFileIndexerTest {

    @Test
    public void test() throws IOException {
	InexFile inexFile = new InexFile("test_data/sample_wiki_file.txt", 1);
	inexFile.title = "hanhan";
	IndexWriterConfig indexWriterConfig = new IndexWriterConfig(
		new StandardAnalyzer());
	indexWriterConfig.setOpenMode(OpenMode.CREATE);
	try (RAMDirectory ramDirectory = new RAMDirectory()) {
	    try (IndexWriter writer = new IndexWriter(ramDirectory,
		    indexWriterConfig)) {
		WikiFileIndexer indexer = new WikiFileIndexer();
		indexer.index(inexFile, writer);
	    }
	    IndexReader reader = DirectoryReader.open(ramDirectory);
	    Document doc = reader.document(0);
	    assertEquals(inexFile.title, doc.get(WikiFileIndexer.TITLE_ATTRIB));
	}
    }

}
