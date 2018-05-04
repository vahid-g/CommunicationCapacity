package cache_enhancement;

import indexing.InexFile;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Logger;

public class WikiIndexUpdater extends IndexManipulator{

    public static final Logger LOGGER = Logger.getLogger(WikiIndexUpdater.class.getName());

    public WikiIndexUpdater(String indexPath, String wikiCountPathFilePath) {
        super(indexPath);
    }

    @Override
    public void addDoc(String docPath) {

    }

    @Override
    public void addDoc(List<String> docPaths) {

    }

    @Override
    public void removeDoc(String docPath) {

    }

    @Override
    public void removeDoc(int docId) {

    }

    @Override
    public void removeDoc(List<String> docPaths) {

    }

    @Override
    public void removeDoc(int[] docIds) {

    }
}
