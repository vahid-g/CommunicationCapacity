package irstyle;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

public class IndexerHelper {

	public static String ID_FIELD = "id";
	public static String TEXT_FIELD = "text";

	public static void indexRS(String idAttrib, String[] textAttribs, IndexWriter iwriter, ResultSet rs)
			throws SQLException, IOException {
		String id = rs.getString(idAttrib);
		StringBuilder answerBuilder = new StringBuilder();
		for (String s : textAttribs) {
			answerBuilder.append(rs.getString(s));
		}
		String answer = answerBuilder.toString();
		Document doc = new Document();
		doc.add(new StoredField(ID_FIELD, id));
		// answer = StringEscapeUtils.unescapeHtml4(answer); // convert html encoded
		// characters to unicode
		doc.add(new TextField(TEXT_FIELD, answer, Store.NO));
		iwriter.addDocument(doc);
	}

}
