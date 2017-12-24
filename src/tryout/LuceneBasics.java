package tryout;

import java.io.StringReader;
import java.util.Iterator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

public class LuceneBasics {

    public static void main(String[] args) throws Exception {
	try3();
    }

    static void try3() throws Exception {
	Analyzer analyzer = new StandardAnalyzer();
	Directory directory = new RAMDirectory();
	IndexWriterConfig config = new IndexWriterConfig(analyzer);
	config.setSimilarity(new BM25Similarity());
	IndexWriter iwriter = new IndexWriter(directory, config);
	Document doc1 = new Document();
	Document doc2 = new Document();
	Document doc3 = new Document();
	final String afield = "random_field";
	doc1.add(new Field(afield, "this is the new Shekh&apos;s", TextField.TYPE_STORED));
	// doc2.add(new Field(field, "shekh's text text text", TextField.TYPE_STORED));
	// doc3.add(new Field(field, "new new new's sh*t shekh",
	// TextField.TYPE_STORED));
	iwriter.addDocument(doc1);
	iwriter.addDocument(doc2);
	iwriter.addDocument(doc3);
	iwriter.close();

	// search the index:
	IndexReader reader = DirectoryReader.open(directory);
	final Fields fields = MultiFields.getFields(reader);
	final Iterator<String> iterator = fields.iterator();

	while (iterator.hasNext()) {
	    final String field = iterator.next();
	    final Terms terms = MultiFields.getTerms(reader, field);
	    final TermsEnum it = terms.iterator();
	    BytesRef term = it.next();
	    while (term != null) {
		System.out.println(term.utf8ToString());
		term = it.next();
	    }
	}
    }

    static void try2() throws Exception {
	Analyzer analyzer = new StandardAnalyzer();
	TokenStream tokenStream = analyzer.tokenStream("field", new StringReader("Text \"text-text\""));
	CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
	tokenStream.reset();
	while (tokenStream.incrementToken()) {
	    System.out.println(termAtt.toString());
	}
	analyzer.close();

    }

    static void try1() throws Exception {
	Analyzer analyzer = new StandardAnalyzer();
	Directory directory = new RAMDirectory();
	IndexWriterConfig config = new IndexWriterConfig(analyzer);
	config.setSimilarity(new BM25Similarity());
	IndexWriter iwriter = new IndexWriter(directory, config);
	Document doc1 = new Document();
	Document doc2 = new Document();
	Document doc3 = new Document();
	final String field = "random_field";
	doc1.add(new Field(field, "this is the new Shekh&apos;s", TextField.TYPE_STORED));
	// doc2.add(new Field(field, "shekh's text text text", TextField.TYPE_STORED));
	// doc3.add(new Field(field, "new new new's sh*t shekh",
	// TextField.TYPE_STORED));
	iwriter.addDocument(doc1);
	iwriter.addDocument(doc2);
	iwriter.addDocument(doc3);
	iwriter.close();

	// search the index:
	IndexReader ireader = DirectoryReader.open(directory);
	System.out.println(ireader.totalTermFreq(new Term(field, "Shekh&apos")));
	System.out.println(ireader.totalTermFreq(new Term(field, "Shekh's")));
	System.out.println(ireader.totalTermFreq(new Term(field, "Shekh")));
	// System.out.println(ireader.getSumDocFreq(field));
	// System.out.println(ireader.getSumTotalTermFreq(field));

	IndexSearcher isearcher = new IndexSearcher(ireader);
	isearcher.setSimilarity(new BM25Similarity()); // Parse a simple
	// query that searches for "text":
	QueryParser parser = new QueryParser(field, new StandardAnalyzer());
	Query query = parser.parse("shekh's");
	// query = new TermQuery(new Term("shekh"));
	ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
	System.out.println("#hits:" + hits.length);
	for (int i = 0; i < hits.length; i++) {
	    Document hitDoc = isearcher.doc(hits[i].doc);
	    System.out.println(hits[i].score + "\t" + hitDoc.get(field));
	    System.out.println(isearcher.explain(query, hits[i].doc));
	    System.out.println(isearcher.explain(query, //
		    hits[i].doc).getDetails()[0].getDetails()[0].getDetails()[0]); //
	    printExp(isearcher.explain(query, hits[i].doc), "-");
	}

	ireader.close();
	directory.close();
    }

    static void printExp(Explanation exp, String prefix) {
	System.out.println(prefix + exp.getDescription() + " ==> " + exp.getValue());
	for (Explanation childExp : exp.getDetails()) {
	    printExp(childExp, prefix + "  ");
	}
    }

}
