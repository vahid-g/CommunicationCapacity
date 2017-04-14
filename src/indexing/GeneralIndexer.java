package indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

public abstract class GeneralIndexer {

	public void buildIndex(Map<String, Integer> fileCountMap, String indexPath,
			float fieldBoost) {
		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(directory, getConfig());
			for (String filePath : fileCountMap.keySet()) {
				indexXmlFile(new File(filePath), writer, 1, fieldBoost);
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

	public void buildIndexBoosted(Map<String, Integer> fileCountMap,
			String indexPath, float fieldBoost) {
		int N = 0;
		for (float n_i : fileCountMap.values()) {
			N += n_i;
		}
		int V = fileCountMap.size();
		float alpha = 1.0f;

		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(directory, getConfig());
			for (String filePath : fileCountMap.keySet()) {
				float count = (float) fileCountMap.get(filePath);
				float smoothed = (count + alpha) / (N + V * alpha);
				indexXmlFile(new File(filePath), writer, smoothed, fieldBoost);
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

	protected IndexWriterConfig getConfig() {
		IndexWriterConfig config;
		config = new IndexWriterConfig(new StandardAnalyzer());
		config.setOpenMode(OpenMode.CREATE);
		config.setRAMBufferSizeMB(1024.00);
		config.setSimilarity(new BM25Similarity());
		return config;
	}

	protected abstract void indexXmlFile(File file, IndexWriter writer,
			float docBoost, float fieldBoost);

}
