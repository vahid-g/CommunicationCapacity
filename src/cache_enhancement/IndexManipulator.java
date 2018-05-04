package cache_enhancement;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Logger;

abstract public class IndexManipulator {
    public static final Logger LOGGER = Logger.getLogger(IndexManipulator.class.getName());

    protected IndexReader indexReader;
    protected IndexWriter indexWriter;
    protected IndexSearcher indexSearcher = null;

    public IndexManipulator(String indexPath) {
        try {
            FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(directory);
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriter = new IndexWriter(directory, indexWriterConfig);
        }
        catch (Exception e) {

        }
    }

    abstract public void addDoc(String docPath);
    abstract public void addDoc(List<String> docPaths);
    abstract public void removeDoc(String docPath);
    abstract public void removeDoc(int docId);
    abstract public void removeDoc(List<String> docPaths);
    abstract public void removeDoc(int[] docIds);
}
