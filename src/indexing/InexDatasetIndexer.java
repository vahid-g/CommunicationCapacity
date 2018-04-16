package indexing;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

public class InexDatasetIndexer {

	private static final Logger LOGGER = Logger.getLogger(GeneralIndexer.class.getName());

	private InexFileIndexer fileIndexer;
	private IndexWriterConfig indexWriterConfig;

	public InexDatasetIndexer(InexFileIndexer fileIndexer) {
		this.fileIndexer = fileIndexer;
	}

	public void buildIndex(List<InexFile> list, String indexPath, Similarity similarity, Analyzer analyzer) {
		indexWriterConfig = new IndexWriterConfig(analyzer);
		indexWriterConfig.setOpenMode(OpenMode.CREATE);
		indexWriterConfig.setRAMBufferSizeMB(1024.00);
		indexWriterConfig.setSimilarity(similarity);
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexWriter writer = new IndexWriter(directory, indexWriterConfig)) {
			int failedIndexCounter = 0;
			for (InexFile ifm : list) {
				if (!fileIndexer.index(ifm, writer)) {
					failedIndexCounter++;
				}
			}
			LOGGER.log(Level.INFO,
					"Number of failed files to index = " + failedIndexCounter + " out of " + list.size() + " files");
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public void parallelBuilIndex(List<InexFile> list, String indexPath, Similarity similarity, Analyzer analyzer) {
		indexWriterConfig = new IndexWriterConfig(analyzer);
		indexWriterConfig.setOpenMode(OpenMode.CREATE);
		indexWriterConfig.setRAMBufferSizeMB(1024.00);
		indexWriterConfig.setSimilarity(similarity);
		int partitionCount = 20;
		int step = (int) (list.size() / partitionCount);
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexWriter writer = new IndexWriter(directory, indexWriterConfig)) {
			List<Integer> partitions = IntStream.rangeClosed(1, partitionCount).boxed().collect(Collectors.toList());
			partitions.parallelStream().forEach(p -> {
				List<InexFile> inexFilesSublist = list.subList((p - 1) * step, p * step);
				for (InexFile i : inexFilesSublist) {
					fileIndexer.index(i, writer);
				}
			});
		} catch (

		IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}
}
