package amazon.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

import indexing.GeneralIndexer;
import indexing.InexFile;

public class AmazonDatasetIndexer {

	private static final Logger LOGGER = Logger.getLogger(GeneralIndexer.class.getName());

	private InexFileIndexer fileIndexer;
	private IndexWriterConfig indexWriterConfig;

	public AmazonDatasetIndexer(InexFileIndexer fileIndexer) {
		this.fileIndexer = fileIndexer;
	}
	
	public void buildIndex(List<InexFile> list, String indexPath) {
		buildIndex(list, indexPath, new BM25Similarity());
	}
	
	public void buildIndex(List<InexFile> list, String indexPath, Similarity similarity) {
		indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
		indexWriterConfig.setOpenMode(OpenMode.CREATE);
		indexWriterConfig.setRAMBufferSizeMB(1024.00);
		indexWriterConfig.setSimilarity(similarity);
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexWriter writer = new IndexWriter(directory, indexWriterConfig)) {
			for (InexFile ifm : list) {
				fileIndexer.index(new File(ifm.path), writer);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

}
