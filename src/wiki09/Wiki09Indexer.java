package wiki09;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import indexing.GeneralIndexer;

public class Wiki09Indexer extends GeneralIndexer {

    protected void indexXmlFile(File file, IndexWriter writer, float weight) {
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
	    doc.add(new StringField(GeneralIndexer.DOCNAME_ATTRIB,
		    FilenameUtils.removeExtension(file.getName()),
		    Field.Store.YES));
	    TextField titleField = new TextField(GeneralIndexer.TITLE_ATTRIB,
		    title, Field.Store.YES);
	    doc.add(titleField);
	    TextField contentField = new TextField(
		    GeneralIndexer.CONTENT_ATTRIB, fileContent, Field.Store.YES);
	    doc.add(contentField);
	    writer.addDocument(doc);
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

}
