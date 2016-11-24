package inex;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

/**
 * @author ghadakcv This class is based on the InexMsnIndexer class
 */
public class InexIndexer {

	static IndexWriterConfig getConfig(boolean append) {
		IndexWriterConfig config;
		config = new IndexWriterConfig(new StandardAnalyzer());
		if (append)
			config.setOpenMode(OpenMode.APPEND);
		else
			config.setOpenMode(OpenMode.CREATE);
		config.setRAMBufferSizeMB(1024.00);
		config.setSimilarity(new BM25Similarity());
		return config;
	}


	public static void buildIndex(String[] datasetFilePaths, String indexPath) {
		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(directory, getConfig(false));
			for (String filePath : datasetFilePaths) {
					indexFile(filePath, writer);
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

	public static void updateIndex(String prevIndexPath, String indexPath) {
		FSDirectory prevIndexDir = null;
		FSDirectory currentDir = null;
		IndexWriter writer = null;
		try {
			prevIndexDir = FSDirectory.open(Paths.get(prevIndexPath));
			currentDir = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(currentDir, getConfig(true));
			writer.addIndexes(prevIndexDir);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (prevIndexDir != null)
				prevIndexDir.close();
			if (currentDir != null)
				currentDir.close();
		}
	}

	static void indexFile(String filepath, IndexWriter writer) {
		File file = new File(filepath);
		try {
			String fileContent = new String(Files.readAllBytes(Paths
					.get(filepath)), StandardCharsets.UTF_8);
			if (isRedirectingFile(fileContent))
				return;
			Document doc = new Document();
			doc.add(new StringField(Experiment.DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			doc.add(new TextField(Experiment.CONTENT_ATTRIB, fileContent,
					Field.Store.YES));
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean isRedirectingFile(String fileContent) {
		int length = fileContent.length() > 8 ? 8 : fileContent.length();
		return (fileContent.substring(0, length).equals("REDIRECT"));
	}

}
