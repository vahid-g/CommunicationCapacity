package indexing;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class PrintFieldTerms {

	public static void main(String[] args) {
		String indexPath = args[0];
		String field = args[1];
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory)) {
			printFieldTerms(field, reader);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void printFieldTerms(String field, IndexReader reader) throws IOException {
		final Terms terms = MultiFields.getTerms(reader, field);
		final TermsEnum it = terms.iterator();
		BytesRef term = it.next();
		while (term != null) {
			System.out.println(term.utf8ToString());
			term = it.next();
		}
	}

}
