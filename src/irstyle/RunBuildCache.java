package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;

public class RunBuildCache {

	public static void main(String[] args) throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			Connection conn = dc.getConnection();
			String tableName = "tbl_article_wiki13";
			String[] textAttribs = new String[] { "title", "text" };
			String cacheTable = "sub_" + tableName.substring(4);
			int pageSize = 10;
			try (Statement stmt = dc.getConnection().createStatement()) {
				stmt.execute("drop table if exists " + cacheTable + ";");
				stmt.execute("create table " + cacheTable + " as select id from " + tableName
						+ " order by popularity desc limit " + pageSize + ";");
				stmt.execute("create index id on " + cacheTable + "(id);");
			}
			String selectTemplate = "select * from " + tableName + " order by popularity desc limit ?, " + pageSize
					+ ";";
			String insertTemplate = "insert into " + cacheTable + " (id) values (?);";
			int offset = pageSize;
			IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
			config.setSimilarity(new BM25Similarity());
			config.setRAMBufferSizeMB(1024);
			config.setOpenMode(OpenMode.CREATE_OR_APPEND);
			Directory directory = FSDirectory.open(Paths.get("???"));
			try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
				try (PreparedStatement selectStmt = conn.prepareStatement(selectTemplate);
						PreparedStatement insertStmt = conn.prepareStatement(insertTemplate)) {
					int count = 0;
					while (true) {
						selectStmt.setInt(1, offset);
						offset += pageSize;
						ResultSet rs = selectStmt.executeQuery();
						while (rs.next()) {
							int id = rs.getInt("id");
							count++;
							insertStmt.setInt(1, id);
							insertStmt.addBatch();
							String text = rs.getString("title") + rs.getString("text");
							IndexerHelper.indexRS("id", textAttribs, indexWriter, rs);
						}
						insertStmt.executeBatch();
						// test partition!
					}
				}
			}
		}
	}

}
