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

public class WikiIndexUpdater {

    public static final Logger LOGGER = Logger.getLogger(WikiIndexUpdater.class.getName());

    private final IndexReader indexReader;
    private final IndexWriter indexWriter;


    public WikiIndexUpdater(String indexPath) throws IOException{
        FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
        indexReader = DirectoryReader.open(directory);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
        indexWriter = new IndexWriter(directory, indexWriterConfig);

    }

    public void add(List<InexFile> inexFiles){

    }

    public void add(InexFile inexFile) {

    }

    public void remove(List<InexFile> inexFiles) {

    }

    public void remove(InexFile inexFile) {

    }

}
