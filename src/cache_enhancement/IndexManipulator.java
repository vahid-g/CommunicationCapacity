package cache_enhancement;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract public class IndexManipulator {
    public static final Logger LOGGER = Logger.getLogger(IndexManipulator.class.getName());

    protected IndexReader indexReader;
    protected IndexWriter indexWriter;
    protected IndexSearcher indexSearcher;

    public IndexManipulator(String indexPath) {
        try {
            FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(directory);
            indexSearcher = new IndexSearcher(indexReader);
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriterConfig.setRAMBufferSizeMB(1024.00);
            indexWriterConfig.setSimilarity(new BM25Similarity());
            indexWriter = new IndexWriter(directory, indexWriterConfig);
        }
        catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    abstract public void addDoc(String docId);
    abstract public void addDoc(List<String> docIds);
    abstract public void removeDoc(String docId);
    abstract public void removeDoc(List<String> docIds);
}
