package tryout;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class LuceneBasics {

    public static void main(String[] args) throws Exception {
	Analyzer analyzer = new StandardAnalyzer();

	// Store the index in memory:
	Directory directory = new RAMDirectory();
	// To store an index on disk, use this instead:
	// Directory directory = FSDirectory.open("/tmp/testindex");
	IndexWriterConfig config = new IndexWriterConfig(analyzer);
	config.setSimilarity(new BM25Similarity());
	IndexWriter iwriter = new IndexWriter(directory, config);
	Document doc1 = new Document();
	Document doc2 = new Document();
	Document doc3 = new Document();
	doc1.add(new Field("fieldname", "this is the text",
		TextField.TYPE_STORED));
	doc2.add(new Field("fieldname", "text text text", TextField.TYPE_STORED));
	doc3.add(new Field("fieldname", "new sh*t text", TextField.TYPE_STORED));
	iwriter.addDocument(doc1);
	iwriter.addDocument(doc2);
	iwriter.addDocument(doc3);
	iwriter.close();

	// Now search the index:
	DirectoryReader ireader = DirectoryReader.open(directory);
	IndexSearcher isearcher = new IndexSearcher(ireader);
	isearcher.setSimilarity(new BM25Similarity());
	// Parse a simple query that searches for "text":
	QueryParser parser = new QueryParser("fieldname", analyzer);
	Query query = parser.parse("new text");
	ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
	// Iterate through the results:
	for (int i = 0; i < hits.length; i++) {
	    Document hitDoc = isearcher.doc(hits[i].doc);
	    System.out.println(hits[i].score + "\t" + hitDoc.get("fieldname"));
	    // System.out.println(isearcher.explain(query, hits[i].doc));
	    // System.out.println(isearcher.explain(query,
	    // hits[i].doc).getDetails()[0].getDetails()[0].getDetails()[0]);
	    printExp(isearcher.explain(query, hits[i].doc), "-");
	}
	ireader.close();
	directory.close();
    }

    static void printExp(Explanation exp, String prefix) {
	System.out.println(prefix + exp.getDescription() + " ==> "
		+ exp.getValue());
	for (Explanation childExp : exp.getDetails()) {
	    printExp(childExp, prefix + "  ");
	}
    }

}
