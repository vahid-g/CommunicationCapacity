package indexing;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class PrintFirstIndexTerm {

	public static void main(String[] args) {
		String indexPath = args[0];
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory)) {
			final Fields fields = MultiFields.getFields(reader);
			final Iterator<String> iterator = fields.iterator();
			while (iterator.hasNext()) {
				final String field = iterator.next();
				System.out.println("filed " + field);
				final Terms terms = MultiFields.getTerms(reader, field);
				final TermsEnum it = terms.iterator();
				BytesRef term = it.next();
				int counter = 0;
				while (term != null) {
					System.out.println(term.utf8ToString());
					term = it.next();
					if (counter++ > 10) break;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
