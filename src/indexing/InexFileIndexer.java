package indexing;

import org.apache.lucene.index.IndexWriter;

public interface InexFileIndexer {
	
	public void index(InexFile inexFile, IndexWriter writer);

}
