package indexing;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

public class PrintIndexStructure {

	public static void main(String[] args) throws IOException {
		String indexPath = args[0];
		System.out.println("Index structure:");
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory)) {
			System.out.println(reader.leaves().size());
		}
	}

}
