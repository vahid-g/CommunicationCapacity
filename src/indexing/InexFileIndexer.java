package indexing;

import org.apache.lucene.index.IndexWriter;

public interface InexFileIndexer {

	public boolean index(InexFile inexFile, IndexWriter writer);

}
