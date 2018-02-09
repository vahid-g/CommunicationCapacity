package tryout;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
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
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

import query.BoostedScoreQuery;

public class LuceneBasics {

    public static void main(String[] args) throws Exception {
	FileOutputStream out = new FileOutputStream("example-config.properties");
	Properties defaultProps = new Properties();
	defaultProps.setProperty("db-url", "jdbc:mysql://engr-db.engr.oregonstate.edu");
	defaultProps.setProperty("db-port", "3306");
	defaultProps.setProperty("db-name", "your-db-name");
	defaultProps.setProperty("password", "****");
	defaultProps.store(out, "---Default Config Properties---");
	out.close();
    }

    // testing document boosting using costume built query class
    static void try5() throws Exception {
	Directory directory = new RAMDirectory();
	Analyzer analyzer = new StandardAnalyzer();
	IndexWriterConfig config = new IndexWriterConfig(analyzer);
	config.setSimilarity(new BM25Similarity());
	IndexWriter iwriter = new IndexWriter(directory, config);
	Document doc1 = new Document();
	Document doc2 = new Document();
	Document doc3 = new Document();
	doc1.add(new StoredField("i", "d-1"));
	doc1.add(
		new Field("f", "this is the new Shekh", TextField.TYPE_STORED));
	doc1.add(new StoredField("weight", 1));
	doc2.add(new StoredField("i", "d-2"));
	doc2.add(
		new Field("f", "this is the new Shekh", TextField.TYPE_STORED));
	doc2.add(new StoredField("weight", 20));
	doc3.add(new StoredField("i", "d-3"));
	doc3.add(new Field("f", "this is the new", TextField.TYPE_STORED));
	doc3.add(new StoredField("weight", 55));
	iwriter.addDocument(doc1);
	iwriter.addDocument(doc2);
	iwriter.addDocument(doc3);
	iwriter.close();
	IndexReader reader = DirectoryReader.open(directory);
	IndexSearcher isearcher = new IndexSearcher(reader);
	isearcher.setSimilarity(new BM25Similarity());
	QueryParser parser = new QueryParser("f", new StandardAnalyzer());
	Query query = parser.parse("new Shekh");
	Query bsq = new BoostedScoreQuery(query, "weight");
	ScoreDoc[] hits = isearcher.search(bsq, 10).scoreDocs;
	for (int i = 0; i < hits.length; i++) {
	    Document hitDoc = isearcher.doc(hits[i].doc);
	    System.out.println(hitDoc.get("i"));
	    System.out.println(isearcher.explain(bsq, hits[i].doc));
	}
    }

    // testing document boosting using FunctionQuery that multiplies the score
    // by the boost
    static void try4() throws Exception {
	Directory directory = new RAMDirectory();
	generateIndex(directory);
	IndexReader reader = DirectoryReader.open(directory);
	IndexSearcher isearcher = new IndexSearcher(reader);
	isearcher.setSimilarity(new BM25Similarity());
	QueryParser parser = new QueryParser("f", new StandardAnalyzer());
	Query query = parser.parse("new Shekh");
	// FunctionScoreQuery fsq = new FunctionScoreQuery(query,
	// DoubleValuesSource.fromDoubleField("w"));
	FunctionQuery fq = new FunctionQuery(new DoubleFieldSource("w"));
	Query csq = new CustomScoreQuery(query, fq);
	ScoreDoc[] hits = isearcher.search(csq, 1000).scoreDocs;
	System.out.println("#hits:" + hits.length);
	for (int i = 0; i < hits.length; i++) {
	    Document hitDoc = isearcher.doc(hits[i].doc);
	    System.out.println(hitDoc.get("i"));
	    System.out.println(isearcher.explain(csq, hits[i].doc));
	}
    }

    // printing the index
    static void try3() throws Exception {
	Directory directory = new RAMDirectory();
	generateIndex(directory);
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

    // printing output of a token stream
    static void try2() throws Exception {
	Analyzer analyzer = new StandardAnalyzer();
	TokenStream tokenStream = analyzer.tokenStream("field",
		new StringReader("Text \"han-han\""));
	CharTermAttribute termAtt = tokenStream
		.addAttribute(CharTermAttribute.class);
	tokenStream.reset();
	while (tokenStream.incrementToken()) {
	    System.out.println(termAtt.toString());
	}
	analyzer.close();
    }

    static void try1() throws Exception {
	Directory directory = new RAMDirectory();
	generateIndex(directory);

	// search the index:
	IndexReader ireader = DirectoryReader.open(directory);
	System.out.println(ireader.totalTermFreq(new Term("f", "Shekh&apos")));
	System.out.println(ireader.totalTermFreq(new Term("f", "Shekh's")));
	System.out.println(ireader.totalTermFreq(new Term("f", "Shekh")));
	System.out.println(ireader.getSumDocFreq("f"));
	System.out.println(ireader.getSumTotalTermFreq("f"));

	IndexSearcher isearcher = new IndexSearcher(ireader);
	isearcher.setSimilarity(new BM25Similarity()); // Parse a simple
	// query that searches for "text":
	QueryParser parser = new QueryParser("f", new StandardAnalyzer());
	Query query = parser.parse("shekh");
	ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
	System.out.println("#hits:" + hits.length);
	for (int i = 0; i < hits.length; i++) {
	    Document hitDoc = isearcher.doc(hits[i].doc);
	    System.out.println(hits[i].score + "\t" + hitDoc.get("f"));
	    System.out.println(isearcher.explain(query, hits[i].doc));
	    System.out.println(isearcher.explain(query, //
		    hits[i].doc).getDetails()[0].getDetails()[0]
			    .getDetails()[0]); //
	    printExp(isearcher.explain(query, hits[i].doc), "-");
	}

	ireader.close();
	directory.close();
    }

    private static void printExp(Explanation exp, String prefix) {
	System.out.println(
		prefix + exp.getDescription() + " ==> " + exp.getValue());
	for (Explanation childExp : exp.getDetails()) {
	    printExp(childExp, prefix + "  ");
	}
    }

    private static void generateIndex(Directory directory) throws IOException {
	Analyzer analyzer = new StandardAnalyzer();
	IndexWriterConfig config = new IndexWriterConfig(analyzer);
	config.setSimilarity(new BM25Similarity());
	IndexWriter iwriter = new IndexWriter(directory, config);
	Document doc1 = new Document();
	Document doc2 = new Document();
	Document doc3 = new Document();
	doc1.add(new StoredField("i", "d-1"));
	doc1.add(
		new Field("f", "this is the new Shekh", TextField.TYPE_STORED));
	doc1.add(new DoubleDocValuesField("weight", 1));
	doc2.add(new StoredField("i", "d-2"));
	doc2.add(
		new Field("f", "this is the new Shekh", TextField.TYPE_STORED));
	doc2.add(new DoubleDocValuesField("weight", 20));
	doc3.add(new StoredField("i", "d-3"));
	doc3.add(new Field("f", "this is the new", TextField.TYPE_STORED));
	doc3.add(new DoubleDocValuesField("weight", 65));
	iwriter.addDocument(doc1);
	iwriter.addDocument(doc2);
	iwriter.addDocument(doc3);
	iwriter.close();
    }

}
