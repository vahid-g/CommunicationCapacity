package wiki13;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import indexing.InexFile;
import indexing.InexFileIndexer;

public class WikiFileIndexer implements InexFileIndexer {

	private static final Logger LOGGER = Logger.getLogger(WikiFileIndexer.class.getName());

	public static final String CONTENT_ATTRIB = "content";
	public static final String DOCNAME_ATTRIB = "name";
	public static final String TITLE_ATTRIB = "title";
	public static final String WEIGHT_ATTRIB = "weight";

	public boolean index(InexFile pct, IndexWriter writer) {
		File file = new File(pct.path);
		if (!file.exists()) {
			return false;
		}
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = StringEscapeUtils.unescapeXml(new String(data, "UTF-8"));
			Document doc = new Document();
			doc.add(new StringField(DOCNAME_ATTRIB, FilenameUtils.removeExtension(file.getName()), Field.Store.YES));
			doc.add(new StoredField(WEIGHT_ATTRIB, pct.weight));
			TextField titleField = new TextField(TITLE_ATTRIB, pct.title, Field.Store.YES);
			doc.add(titleField);
			TextField contentField = new TextField(CONTENT_ATTRIB, fileContent, Field.Store.NO);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString() + "\n" + e.fillInStackTrace());
			return false;
		}
		return true;
	}

	protected void indexXmlFile(File file, IndexWriter writer) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			if (fileContent.contains("<listitem>REDIRECT")) {
				return;
			}
			Pattern p = Pattern.compile("<article id=\"\\d*\" title=\"(.*?)\">", Pattern.DOTALL);
			Matcher m = p.matcher(fileContent);
			String title = "";
			if (m.find())
				title = m.group(1);
			else
				LOGGER.log(Level.INFO, "Title not found in " + file.getName());
			fileContent = fileContent.replaceAll("<[^>]*>", " ").trim();
			Document doc = new Document();
			doc.add(new StringField(DOCNAME_ATTRIB, FilenameUtils.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(TITLE_ATTRIB, title, Field.Store.YES);
			doc.add(titleField);
			TextField contentField = new TextField(CONTENT_ATTRIB, fileContent, Field.Store.YES);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, e.toString() + "\n" + e.fillInStackTrace());
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString() + "\n" + e.fillInStackTrace());
		}
	}

}
