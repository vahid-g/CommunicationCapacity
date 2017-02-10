package inex13;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
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

/**
 * @author ghadakcv This class is based on the InexMsnIndexer class
 */
public class InexIndexer {

	public static void buildIndex(
			List<Map.Entry<String, Integer>> fileCountList, String indexPath) {
		// computing smoothing params
		int N = 0;
		for (Map.Entry<String, Integer> entry : fileCountList) {
			N += entry.getValue();
		}
		int V = fileCountList.size();
		float alpha = 1.0f;

		// indexing
		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			IndexWriterConfig config = new IndexWriterConfig(
					new StandardAnalyzer());
			config.setOpenMode(OpenMode.CREATE);
			config.setRAMBufferSizeMB(1024.00);
			writer = new IndexWriter(directory, config);
			for (Map.Entry<String, Integer> entry : fileCountList) {
				float count = (float) entry.getValue();
				float smoothed = (count + alpha) / (N + V * alpha);
				indexXmlFileWithWeight(new File(entry.getKey()), writer,
						smoothed);
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

	static void indexXmlFileWithWeight(File file, IndexWriter writer,
			float weight) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			if (fileContent.contains("<listitem>REDIRECT")) {
				return;
			}
			Pattern p = Pattern.compile(".*<title>(.*?)</title>.*",
					Pattern.DOTALL);
			Matcher m = p.matcher(fileContent);
			m.find();
			String title = "";
			if (m.matches())
				title = m.group(1);
			else
				System.out.println("!!! title not found in " + file.getName());
			fileContent = fileContent.replaceAll("<[^>]*>", " ").trim();
			
			Document doc = new Document();
			doc.add(new StringField(inex09.InexIndexer.DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(inex09.InexIndexer.TITLE_ATTRIB, title,
					Field.Store.YES);
			titleField.setBoost(weight);
			doc.add(titleField);
			TextField contentField = new TextField(inex09.InexIndexer.CONTENT_ATTRIB, fileContent,
					Field.Store.YES);
			contentField.setBoost(weight);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
