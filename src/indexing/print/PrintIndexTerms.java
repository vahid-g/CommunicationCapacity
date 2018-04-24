package indexing.print;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.FSDirectory;

public class PrintIndexTerms {

	public static void main(String[] args) {
		String indexPath = args[0];
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory)) {
			final Fields fields = MultiFields.getFields(reader);
			final Iterator<String> iterator = fields.iterator();
			while (iterator.hasNext()) {
				final String field = iterator.next();
				System.out.println("filed " + field);
				PrintFieldTerms.printFieldTerms(field, reader);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
