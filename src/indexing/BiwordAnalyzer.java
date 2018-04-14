package indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class BiwordAnalyzer extends Analyzer {
	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		Tokenizer source = new StandardTokenizer();
		TokenStream filter = new LowerCaseFilter(source);
		ShingleFilter sf = new ShingleFilter(filter);
		sf.setOutputUnigrams(false);
		return new TokenStreamComponents(source, sf);
	}
}
