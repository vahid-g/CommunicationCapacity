package tryout;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class LuceneAnalyzer {
	
	public static void main(String[] args) throws IOException {
		try4();
	}

	static void try4() throws IOException {
		Analyzer anal = new Analyzer() {
			@Override
			protected TokenStreamComponents createComponents(String fieldName) {
				Tokenizer source = new StandardTokenizer();
				TokenStream filter = new LowerCaseFilter(source);
				ShingleFilter sf = new ShingleFilter(filter);
				sf.setOutputUnigrams(false);
				return new TokenStreamComponents(source, sf);
			}
		};
		TokenStream ts = anal.tokenStream("f", "<X 'Google's C.E.O' a");
		CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
		ts.reset();
		while (ts.incrementToken()) {
			System.out.println(termAtt.toString());
		}
		anal.close();
	}

	static void try3() throws IOException {
		final StandardTokenizer src = new StandardTokenizer();
		src.setReader(new StringReader("hanhan olde? pokh"));
		TokenStream tok = new LowerCaseFilter(src);
		ShingleFilter sf = new ShingleFilter(tok);
		sf.setOutputUnigrams(false);
		CharTermAttribute termAtt = sf.addAttribute(CharTermAttribute.class);
		sf.reset();
		while (sf.incrementToken()) {
			System.out.println(termAtt.toString());
		}
		sf.close();
		src.setReader(new StringReader("god is coming"));
		sf.reset();
		while (sf.incrementToken()) {
			System.out.println(termAtt.toString());
		}
		sf.close();
	}

	// bi-word tokenizing
	static void try2() throws IOException {
		Analyzer analyzer = new WhitespaceAnalyzer();
		TokenStream ts = analyzer.tokenStream("f1", new StringReader("hanhan olde? XXX"));
		ShingleFilter sf = new ShingleFilter(ts);
		CharTermAttribute termAtt = sf.addAttribute(CharTermAttribute.class);
		sf.setOutputUnigrams(false);
		sf.reset();
		while (sf.incrementToken()) {
			System.out.println(termAtt.toString());
		}
		sf.close();
		analyzer.close();
	}

	// testing standard analyzer
	static void try1() throws IOException {
		Analyzer anal = new StandardAnalyzer();
		TokenStream ts = anal.tokenStream("f", "<X 'Google's C.E.O' a");
		CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
		ts.reset();
		while (ts.incrementToken()) {
			System.out.println(termAtt.toString());
		}
		anal.close();
	}
}
