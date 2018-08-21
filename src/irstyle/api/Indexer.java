package irstyle.api;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import irstyle.DatabaseHelper;
import irstyle.RelationalWikiIndexer;

public class Indexer {

	public static void indexCompTable(DatabaseConnection dc, String tableName, int percentage, String[] textAttribs,
			String popularityAttrib) throws IOException, SQLException {
		String indexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + tableName + "/c" + percentage;
		double count = DatabaseHelper.tableSize(tableName, dc.getConnection());
		int limit = (int) Math.floor(count - ((percentage * count) / 100.0));
		indexTable(dc, indexPath, tableName, textAttribs, limit, popularityAttrib, true, getIndexWriterConfig());
	}

	public static IndexWriterConfig getIndexWriterConfig() {
		return getIndexWriterConfig(new StandardAnalyzer());
	}

	public static IndexWriterConfig getIndexWriterConfig(Analyzer analyzer) {
		IndexWriterConfig config;
		config = new IndexWriterConfig(analyzer);
		config.setSimilarity(new BM25Similarity());
		config.setRAMBufferSizeMB(1024);
		return config;
	}

	public static void indexTable(DatabaseConnection dc, String indexPath, String table, String[] textAttribs, int limit,
			String popularity, boolean ascending, IndexWriterConfig config) throws IOException, SQLException {
		File indexFile = new File(indexPath);
		if (!indexFile.exists()) {
			indexFile.mkdirs();
		}
		Directory directory = FSDirectory.open(Paths.get(indexFile.getAbsolutePath()));
		try (IndexWriter iwriter = new IndexWriter(directory, config)) {
			try (Statement stmt = dc.getConnection().createStatement()) {
				stmt.setFetchSize(Integer.MIN_VALUE);
				String attribs = "id";
				for (String s : textAttribs) {
					attribs += "," + s;
				}
				attribs += ", popularity ";
				String sql = "select " + attribs + " from " + table + " order by " + popularity + " desc limit " + limit
						+ ";";
				if (ascending) {
					sql = "select " + attribs + " from " + table + " order by " + popularity + " asc limit " + limit
							+ ";";
				}
				System.out.println(sql);
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					Indexer.indexRS("id", textAttribs, iwriter, rs);
				}
			}
		}
	}

	public static void indexTable(DatabaseConnection dc, IndexWriter indexWriter, String table, String[] textAttribs,
			int limit, String popularity, boolean ascending) throws IOException, SQLException {
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			String attribs = "id";
			for (String s : textAttribs) {
				attribs += "," + s;
			}
			attribs += ", popularity ";
			String sql = "select " + attribs + " from " + table + " order by " + popularity + " desc limit " + limit
					+ ";";
			if (ascending) {
				sql = "select " + attribs + " from " + table + " order by " + popularity + " asc limit " + limit + ";";
			}
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				Indexer.indexRS("id", textAttribs, indexWriter, rs);
			}
		}
	}

	// Make an index on table considering each text attrib as a field
	public static void indexTableAttribs(DatabaseConnection dc, IndexWriter indexWriter, String table, String[] textAttribs,
			int limit, String popularity, boolean ascending) throws IOException, SQLException {
		try (Statement stmt = dc.getConnection().createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			String attribs = "id";
			for (String s : textAttribs) {
				attribs += "," + s;
			}
			attribs += ", popularity ";
			String sql = "select " + attribs + " from " + table + " order by " + popularity + " desc limit " + limit
					+ ";";
			if (ascending) {
				sql = "select " + attribs + " from " + table + " order by " + popularity + " asc limit " + limit + ";";
			}
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				Indexer.indexRSWithAttribs("id", textAttribs, indexWriter, rs);
			}
		}
	}

	public static void indexRS(String idAttrib, String[] textAttribs, IndexWriter iwriter, ResultSet rs)
			throws SQLException, IOException {
		StringBuilder answerBuilder = new StringBuilder();
		for (String s : textAttribs) {
			answerBuilder.append(rs.getString(s));
		}
		String answer = answerBuilder.toString();
		Document doc = new Document();
		doc.add(new StoredField(RelationalWikiIndexer.ID_FIELD, rs.getString(idAttrib)));
		// answer = StringEscapeUtils.unescapeHtml4(answer); // convert html encoded
		// characters to unicode
		doc.add(new TextField(RelationalWikiIndexer.TEXT_FIELD, answer, Store.NO));
		doc.add(new StoredField(RelationalWikiIndexer.WEIGHT_FIELD, rs.getInt("popularity")));
		iwriter.addDocument(doc);
	}

	public static void indexRSWithAttribs(String idAttrib, String[] textAttribs, IndexWriter iwriter, ResultSet rs)
			throws SQLException, IOException {
		Document doc = new Document();
		doc.add(new StoredField(RelationalWikiIndexer.ID_FIELD, rs.getString(idAttrib)));
		doc.add(new StoredField(RelationalWikiIndexer.WEIGHT_FIELD, rs.getInt("popularity")));
		for (String attrib : textAttribs) {
			doc.add(new TextField(attrib, rs.getString(attrib), Store.NO));
	
		}
		iwriter.addDocument(doc);
	
	}

}
