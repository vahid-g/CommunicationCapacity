package wiki_inex13;

import indexing.GeneralIndexer;
import indexing.InexFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

/**
 * @author ghadakcv This class is based on the InexMsnIndexer class
 */
public class Wiki13Indexer extends GeneralIndexer {

	public static final Logger LOGGER = Logger.getLogger(Wiki13Indexer.class
			.getName());

	protected void indexXmlFile(File file, IndexWriter writer, float docBoost) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			if (fileContent.contains("<listitem>REDIRECT")) {
				return;
			}
			Pattern p = Pattern.compile(
					"<article id=\"\\d*\" title=\"(.*?)\">", Pattern.DOTALL);
			Matcher m = p.matcher(fileContent);
			String title = "";
			if (m.find())
				title = m.group(1);
			else
				LOGGER.log(Level.INFO, "Title not found in " + file.getName());
			fileContent = fileContent.replaceAll("<[^>]*>", " ").trim();
			Document doc = new Document();
			doc.add(new StringField(indexing.GeneralIndexer.DOCNAME_ATTRIB,
					FilenameUtils.removeExtension(file.getName()),
					Field.Store.YES));
			TextField titleField = new TextField(
					indexing.GeneralIndexer.TITLE_ATTRIB, title,
					Field.Store.YES);
			doc.add(titleField);
			TextField contentField = new TextField(
					indexing.GeneralIndexer.CONTENT_ATTRIB, fileContent,
					Field.Store.YES);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, e.toString() + "\n" + e.fillInStackTrace());
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString() + "\n" + e.fillInStackTrace());
		}
	}

	public static void buildIndexOnText(List<InexFile> fileCountList,
			String indexPath, float[] gamma) {
		buildIndexOnText(fileCountList, indexPath, gamma,
				new ClassicSimilarity());
	}

	public static void buildIndexOnText(List<InexFile> fileCountList,
			String indexPath, float[] gamma, Similarity similarity) {
		// indexing
		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			IndexWriterConfig config = getIndexWriterConfig().setSimilarity(
					similarity);
			writer = new IndexWriter(directory, config);
			for (InexFile entry : fileCountList) {
				indexTxtFileWithWeight(entry, writer);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString() + "\n" + e.fillInStackTrace());
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE,
							e.toString() + "\n" + e.fillInStackTrace());
				}
			if (directory != null)
				try {
					directory.close();
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.getMessage());
				}
		}
	}

	static void indexTxtFileWithWeight(InexFile pct, IndexWriter writer) {
		File file = new File(pct.path);
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			Document doc = new Document();
			doc.add(new StringField(indexing.GeneralIndexer.DOCNAME_ATTRIB,
					FilenameUtils.removeExtension(file.getName()),
					Field.Store.YES));
			TextField titleField = new TextField(
					indexing.GeneralIndexer.TITLE_ATTRIB, pct.title,
					Field.Store.YES);
			doc.add(titleField);
			TextField contentField = new TextField(
					indexing.GeneralIndexer.CONTENT_ATTRIB, fileContent,
					Field.Store.YES);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (NoSuchFileException e) {
			LOGGER.log(Level.WARNING, "File not found: " + pct.title + " "
					+ pct.path);
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, e.toString() + "\n" + e.fillInStackTrace());
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString() + "\n" + e.fillInStackTrace());
		}
	}
	
	// code for smoothed document boosting:
	// int N = 0;
	// for (InexFile entry : fileCountList) {
	// N += entry.weight;
	// }
	// int V = fileCountList.size();
	// float alpha = 1.0f;
	// float smoothed = (count + alpha) / (N + V * alpha);

}
