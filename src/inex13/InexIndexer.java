package inex13;

import inex13.ClusterMsnExperiment.PathCountTitle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
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
import org.apache.lucene.store.FSDirectory;

/**
 * @author ghadakcv This class is based on the InexMsnIndexer class
 */
public class InexIndexer {
	
	static final Logger LOGGER = Logger
			.getLogger(InexIndexer.class.getName());

	public static void buildIndex(
			List<PathCountTitle> fileCountList, String indexPath, float gamma) {
		// computing smoothing params
		int N = 0;
		for (PathCountTitle entry : fileCountList) {
			N += entry.visitCount;
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
			for (PathCountTitle entry : fileCountList) {
				float count = (float) entry.visitCount;
				float smoothed = (count + alpha) / (N + V * alpha);
				indexXmlFileWithWeight(new File(entry.path), writer,
						smoothed, gamma);
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
	
	public static void buildIndexOnText(
			List<PathCountTitle> fileCountList, String indexPath, float gamma) {
		// computing smoothing params
		int N = 0;
		for (PathCountTitle entry : fileCountList) {
			N += entry.visitCount;
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
			for (PathCountTitle entry : fileCountList) {
				float count = (float) entry.visitCount;
				float smoothed = (count + alpha) / (N + V * alpha);
				indexTxtFileWithWeight(entry, writer,
						smoothed, gamma);
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
	
	static void indexTxtFileWithWeight(PathCountTitle pct, IndexWriter writer,
			float weight, float gamma) {
		File file = new File(pct.path);
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			// if (fileContent.contains("REDIRECT")) {
			// return;
			// }
			Document doc = new Document();
			doc.add(new StringField(inex09.InexIndexer.DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(inex09.InexIndexer.TITLE_ATTRIB, pct.title,
					Field.Store.YES);
			titleField.setBoost(gamma * weight);
			doc.add(titleField);
			TextField contentField = new TextField(inex09.InexIndexer.CONTENT_ATTRIB, fileContent,
					Field.Store.YES);
			contentField.setBoost((1 - gamma) * weight);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void indexXmlFileWithWeight(File file, IndexWriter writer,
			float weight, float gamma) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			if (fileContent.contains("<listitem>REDIRECT")) {
				return;
			}
			Pattern p = Pattern.compile("<article id=\"\\d*\" title=\"(.*?)\">",
					Pattern.DOTALL);
			Matcher m = p.matcher(fileContent);
			String title = "";
			if (m.find())
				title = m.group(1);
			else
				LOGGER.log(Level.INFO, "Title not found in " + file.getName());
			fileContent = fileContent.replaceAll("<[^>]*>", " ").trim();
			Document doc = new Document();
			doc.add(new StringField(inex09.InexIndexer.DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(inex09.InexIndexer.TITLE_ATTRIB, title,
					Field.Store.YES);
			titleField.setBoost(gamma * weight);
			doc.add(titleField);
			TextField contentField = new TextField(inex09.InexIndexer.CONTENT_ATTRIB, fileContent,
					Field.Store.YES);
			contentField.setBoost((1 - gamma) * weight);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
