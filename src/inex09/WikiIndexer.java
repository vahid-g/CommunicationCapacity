package inex09;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class WikiIndexer {

	public static final String CONTENT_ATTRIB = "content";
	public static final String DOCNAME_ATTRIB = "name";
	public static final String TITLE_ATTRIB = "title";

	private static IndexWriterConfig getConfig() {
		IndexWriterConfig config;
		config = new IndexWriterConfig(new StandardAnalyzer());
		config.setOpenMode(OpenMode.CREATE);
		config.setRAMBufferSizeMB(1024.00);
		config.setSimilarity(new BM25Similarity());
		return config;
	}

	static void buildIndex(Map<String, Integer> fileCountMap,
			String indexPath) {
		buildIndex(fileCountMap, indexPath, 0.5f);
	}

	static void buildIndex(Map<String, Integer> fileCountMap,
			String indexPath, float gamma) {
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
				indexXmlFileWithWeight(new File(filePath), writer, smoothed,
						gamma);
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

	static void buildIndexWless(Map<String, Integer> fileCountMap,
			String indexPath, float gamma) {
		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(directory, getConfig());
			for (String filePath : fileCountMap.keySet()) {
				indexXmlFileWithWeight(new File(filePath), writer, 1, gamma);
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

	private static void indexXmlFileWithWeight(File file, IndexWriter writer,
			float weight, float gamma) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			int length = fileContent.length() > 8 ? 8 : fileContent.length();
			if (fileContent.substring(0, length).equals("REDIRECT")) {
				return;
			}
			Pattern p = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
			Matcher m = p.matcher(fileContent);
			String title = "";
			if (m.find())
				title = m.group(1);
			else
				System.out.println("!!! title not found in " + file.getName());
			title.replaceAll("<[^>]*>", " ").replaceAll("\n", " ")
					.replaceAll("\r", " ").trim();
			fileContent = fileContent.replaceAll("<[^>]*>", " ").trim();
			Document doc = new Document();
			doc.add(new StringField(DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(TITLE_ATTRIB, title,
					Field.Store.YES);
			titleField.setBoost(weight * gamma);
			doc.add(titleField);
			TextField contentField = new TextField(CONTENT_ATTRIB, fileContent,
					Field.Store.YES);
			contentField.setBoost(weight * (1 - gamma));
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
