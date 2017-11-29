package amazon.indexing;

import java.io.File;

import org.apache.lucene.index.IndexWriter;

public interface InexFileIndexer {
	
	public void index(File file, IndexWriter writer);

}
