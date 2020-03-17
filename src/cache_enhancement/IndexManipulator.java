package cache_enhancement;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract public class IndexManipulator {
    public static final Logger LOGGER = Logger.getLogger(IndexManipulator.class.getName());

    protected IndexWriter indexWriter;
    public final String indexPath;

    public IndexManipulator(String path) {
        indexPath = path;
        try {
            FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriterConfig.setRAMBufferSizeMB(10.00);
            indexWriterConfig.setSimilarity(new BM25Similarity());
            indexWriter = new IndexWriter(directory, indexWriterConfig);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public void open() {
        if (!indexWriter.isOpen()) {
            try {
                FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
                IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
                indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
                indexWriterConfig.setRAMBufferSizeMB(1024.00);
                indexWriterConfig.setSimilarity(new BM25Similarity());
                indexWriter = new IndexWriter(directory, indexWriterConfig);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    public void close() {
        if (indexWriter.isOpen()) {
            try {
                indexWriter.close();
            }
            catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    public void commit() {
        try {
            indexWriter.commit();
        }
        catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public boolean isOpen() {
        return indexWriter.isOpen();
    }

    abstract public boolean addDoc(String docId);
    abstract public int addDoc(List<String> docIds);
    abstract public boolean removeDoc(String docId);
    abstract public int removeDoc(List<String> docIds);
}
