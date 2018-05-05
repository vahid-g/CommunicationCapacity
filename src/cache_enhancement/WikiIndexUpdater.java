package cache_enhancement;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import wiki13.WikiFileIndexer;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class WikiIndexUpdater extends IndexManipulator{

    public static final Logger LOGGER = Logger.getLogger(WikiIndexUpdater.class.getName());

    public WikiIndexUpdater(String indexPath, String wikiCountPathFilePath) {
        super(indexPath);
    }

    public void search(String queryStr, int hitsPerPage) throws ParseException, IOException {
        Query q = new QueryParser(WikiFileIndexer.CONTENT_ATTRIB, new StandardAnalyzer()).parse(queryStr);
        TopDocs docs = indexSearcher.search(q, hitsPerPage);
        ScoreDoc[] hits = docs.scoreDocs;
        System.out.println("Found " + hits.length + " hits.");
        for(int i=0; i<hits.length; ++i) {
            int docId = hits[i].doc;
            Document d = indexSearcher.doc(docId);
            System.out.println((i + 1) + ". " +
                    d.get(WikiFileIndexer.DOCNAME_ATTRIB) + "\t" +
                    d.get(WikiFileIndexer.TITLE_ATTRIB) + "\t" +
                    d.get(WikiFileIndexer.WEIGHT_ATTRIB));
        }
    }

    @Override
    public void addDoc(String docId) {

    }

    @Override
    public void addDoc(List<String> docIds) {

    }

    @Override
    public void removeDoc(String docId) {

    }

    @Override
    public void removeDoc(List<String> docIds) {

    }
}
