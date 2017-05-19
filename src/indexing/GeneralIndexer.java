package indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

public abstract class GeneralIndexer {

	public static final String CONTENT_ATTRIB = "content";
	public static final String DOCNAME_ATTRIB = "name";
	public static final String TITLE_ATTRIB = "title";
	
	public void buildIndex(List<InexFile> list, String indexPath,
			float... fieldBoost) {
		buildIndex(list, indexPath, new DefaultSimilarity(), fieldBoost);
	}

	public void buildIndex(List<InexFile> list, String indexPath, Similarity similarity,
			float... fieldBoost) {
		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			IndexWriterConfig iwc = getIndexWriterConfig().setSimilarity(similarity);
			writer = new IndexWriter(directory, iwc);
			for (InexFile ifm : list) {
				indexXmlFile(new File(ifm.path), writer, 1, fieldBoost);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (directory != null)
				directory.close();
		}
	}
	
	public void buildIndexDocBoosted(List<InexFile> fileList,
			String indexPath, float[] fieldBoosts) {
		int N = 0;
		for (InexFile inexFile : fileList) {
			N += inexFile.weight;
		}
		int V = fileList.size();
		float alpha = 1.0f;

		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(directory, getIndexWriterConfig());
			for (InexFile inexFile : fileList) {
				float count = (float) inexFile.weight;
				float smoothed = (count + alpha) / (N + V * alpha);
				indexXmlFile(new File(inexFile.path), writer, smoothed, fieldBoosts);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (directory != null)
				directory.close();
		}
	}

	protected static IndexWriterConfig getIndexWriterConfig() {
		IndexWriterConfig config;
		config = new IndexWriterConfig(new StandardAnalyzer());
		config.setOpenMode(OpenMode.CREATE);
		config.setRAMBufferSizeMB(1024.00);
		return config;
	}
	
	protected abstract void indexXmlFile(File file, IndexWriter writer,
			float docBoost, float[] fieldBoost);

}
