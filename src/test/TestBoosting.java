package test;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class TestBoosting {

	IndexWriter indexWriter;

	public static void main(String[] args) throws IOException, ParseException {
		Analyzer analyzer = new StandardAnalyzer();

	    // Store the index in memory:
	    Directory directory = new RAMDirectory();
	    // To store an index on disk, use this instead:
	    //Directory directory = FSDirectory.open("/tmp/testindex");
	    IndexWriterConfig config = new IndexWriterConfig(analyzer);
	    IndexWriter iwriter = new IndexWriter(directory, config);
	    Document doc = new Document();
	    Field f0 = new Field("id", "1", StringField.TYPE_STORED);
	    Field f1 = new Field("f1", "ganjikh hassan, hanhan olde?", TextField.TYPE_STORED);
	    Field f2 = new Field("f2", "dardashti", TextField.TYPE_STORED);
	    f1.setBoost(2);
	    f2.setBoost(2);
	    doc.add(f0);
	    doc.add(f1);
	    doc.add(f2);
	    iwriter.addDocument(doc);
	    doc = new Document();
	    f0 = new Field("id", "2", StringField.TYPE_STORED);
	    f1 = new Field("f1", "hassan", TextField.TYPE_STORED);
//	    f1.setBoost(2);
	    f2 = new Field("f2", "dardashti", TextField.TYPE_STORED);
	    doc.add(f0);
	    doc.add(f1);
	    doc.add(f2);
	    iwriter.addDocument(doc);
	    iwriter.close();
	    
	    // Now search the index:
	    DirectoryReader ireader = DirectoryReader.open(directory);
	    IndexSearcher isearcher = new IndexSearcher(ireader);
	    // Parse a simple query that searches for "text":
	    BooleanQuery.Builder builder = new BooleanQuery.Builder();
	    QueryParser parser = new QueryParser("f1", new StandardAnalyzer());
	    builder.add(parser.parse("hassan"), BooleanClause.Occur.MUST);
	    // parser = new QueryParser("f2", new StandardAnalyzer());
	    // builder.add(parser.parse("dardashti"), BooleanClause.Occur.MUST);
	    Query query = builder.build();
	    ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
	    // Iterate through the results:
	    for (int i = 0; i < hits.length; i++) {
	      Document hitDoc = isearcher.doc(hits[i].doc);
	      System.out.println(hitDoc.get("id"));
	    }
	    ireader.close();
	    directory.close();
	}
	
}
