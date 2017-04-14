package imdb;

import indexing.GeneralIndexer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

public class ImdbIndexer extends GeneralIndexer {

	@Override
	protected void indexXmlFile(File file, IndexWriter writer, float smoothed,
			float gamma) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			String textContent = fileContent.replaceAll("<[^>]*>", "").trim();
			Document doc = new Document();
			doc.add(new StringField("FILE_NAME", FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			TextField contentField = new TextField("TEXT", textContent,
					Field.Store.YES);
			doc.add(contentField);
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
