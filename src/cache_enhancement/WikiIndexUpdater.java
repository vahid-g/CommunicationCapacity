package cache_enhancement;

import indexing.InexFile;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import wiki13.WikiFileIndexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WikiIndexUpdater extends IndexManipulator{

    public static final Logger LOGGER = Logger.getLogger(WikiIndexUpdater.class.getName());
    public Map<String, String> docNumberInfo;

    public WikiIndexUpdater(String indexPath, String wikiCountPathFilePath) {
        super(indexPath);
        docNumberInfo = new HashMap<>();
        File countPath = new File(wikiCountPathFilePath);
        try {
            FileReader fileReader = new FileReader(countPath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;
            int i = 1;
            while ((line = bufferedReader.readLine()) != null) {
                if (!line.contains(","))
                    continue;

                String path = line.split(",", 1)[0];
                String[] subPaths = path.split("/", 0);
                String docName = subPaths[subPaths.length-1];
                final String docNum = docName.replaceAll("[^0-9]", "");
                docNumberInfo.put(docName, line);
                i++;
                if (i>100)
                    break;
            }
        }
        catch (Exception e){
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
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

    protected InexFile makeInexFile(String csvTextLine) {

        String[] fields = csvTextLine.split(",");
        String path = fields[0];
        Double count = Double.parseDouble(fields[1].trim());

        if (fields.length == 3) {
            String title = fields[2].trim();
            return (new InexFile(path, count, title));
        } else {
            return (new InexFile(path, count));
        }
    }

    @Override
    public boolean addDoc(String docId) {
        InexFile inexFile = makeInexFile(docNumberInfo.get(docId));
        WikiFileIndexer wikiFileIndexer = new WikiFileIndexer();
        boolean failed = !wikiFileIndexer.index(inexFile, indexWriter);
        return failed;
    }

    @Override
    public int addDoc(List<String> docIds) {
        int failed = 0;
        for(String docId : docIds){
            if(addDoc(docId))
                failed += 1;
        }
        return failed;
    }

    @Override
    public boolean removeDoc(String docId) {
        return false;
    }

    @Override
    public int removeDoc(List<String> docIds) {
        return 0;
    }

}

