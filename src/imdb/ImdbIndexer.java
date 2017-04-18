package imdb;

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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import indexing.GeneralIndexer;
import wiki_inex09.Wiki09Indexer;

public class ImdbIndexer extends GeneralIndexer {
	
	static final Logger LOGGER = Logger.getLogger(ImdbIndexer.class.getName());

	@Override
	protected void indexXmlFile(File file, IndexWriter writer, float smoothed,
			float gamma) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			Pattern ptr = Pattern.compile("<title>([^<]*)</title>");
			Matcher mtr = ptr.matcher(fileContent);
			String title = "";
			if (mtr.find()){
				title = mtr.group(1).trim();
			} else {
				LOGGER.log(Level.WARNING, "title not found in: " + file.getName());
			}
			String textContent = fileContent.replaceAll("<[^>]*>", "").trim();
			Document doc = new Document();
			doc.add(new StringField(Wiki09Indexer.DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			TextField titleField = new TextField(Wiki09Indexer.TITLE_ATTRIB, title, Field.Store.YES);
			titleField.setBoost(gamma * smoothed);
			TextField contentField = new TextField(Wiki09Indexer.CONTENT_ATTRIB, textContent,
					Field.Store.YES);
			contentField.setBoost((1 - gamma) * smoothed);
			doc.add(titleField);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
