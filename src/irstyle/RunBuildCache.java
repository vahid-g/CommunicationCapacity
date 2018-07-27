package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiFilesPaths;

public class RunBuildCache {

	public static void main(String[] args) throws IOException, SQLException, ParseException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			String tableName = "tbl_article_wiki13";
			String[] textAttribs = new String[] { "title", "text" };
			String cacheTable = "sub_" + tableName.substring(4);
			String indexPath = "/data/ghadakcv/wikipedia/" + cacheTable;
			int pageSize = 100000;
			Connection conn = dc.getConnection();
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("drop table if exists " + cacheTable + ";");
				stmt.execute("create table " + cacheTable + " as select id from " + tableName
						+ " order by popularity desc limit " + pageSize + ";");
				stmt.execute("create index id on " + cacheTable + "(id);");
			}
			String selectTemplate = "select * from " + tableName + " order by popularity desc limit ?, " + pageSize
					+ ";";
			String insertTemplate = "insert into " + cacheTable + " (id) values (?);";
			IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
			config.setSimilarity(new BM25Similarity());
			config.setRAMBufferSizeMB(1024);
			config.setOpenMode(OpenMode.CREATE_OR_APPEND);
			Directory directory = FSDirectory.open(Paths.get(indexPath));
			WikiFilesPaths paths = WikiFilesPaths.getMaplePaths();
			List<ExperimentQuery> queries = null;
			queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(), paths.getMsnQrelFilePath());
			queries = queries.subList(0, 50);
			int topDocs = 100;
			try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
				try (PreparedStatement selectStmt = conn.prepareStatement(selectTemplate);
						PreparedStatement insertStmt = conn.prepareStatement(insertTemplate)) {
					double prevAcc = 0;
					double acc = 0;
					int offset = pageSize;
					int loop = 1;
					while (true) {
						System.out.println("Iteration " + loop++);
						selectStmt.setInt(1, offset);
						offset += pageSize;
						ResultSet rs = selectStmt.executeQuery();
						// updating cache and index
						System.out.println("  reading new cache data..");
						List<Document> docs = new ArrayList<Document>();
						while (rs.next()) {
							int id = rs.getInt("id");
							insertStmt.setInt(1, id);
							insertStmt.addBatch();
							String text = "";
							for (String attrib : textAttribs) {
								text += rs.getString(attrib);
							}
							Document doc = new Document();
							doc.add(new StoredField("id", id));
							doc.add(new TextField("text", text, Store.NO));
							docs.add(doc);
						}
						System.out.println("  updating cache table..");
						insertStmt.executeBatch();
						System.out.println("  updating cache index..");
						indexWriter.addDocuments(docs);
						indexWriter.flush();
						// indexWriter.commit();
						// test partition!
						System.out.println("  testing new cache..");
						List<QueryResult> queryResults = new ArrayList<QueryResult>();
						try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
							System.out.println("  index size: " + reader.numDocs());
							IndexSearcher searcher = new IndexSearcher(reader);
							QueryParser qp = new QueryParser("text", new StandardAnalyzer());
							for (ExperimentQuery q : queries) {
								QueryResult result = new QueryResult(q);
								Query query = qp.parse(QueryParser.escape(q.getText()));
								ScoreDoc[] scoreDocHits = searcher.search(query, topDocs).scoreDocs;
								for (int j = 0; j < Math.min(topDocs, scoreDocHits.length); j++) {
									Document doc = reader.document(scoreDocHits[j].doc);
									String docId = doc.get("id");
									result.addResult(docId, "no title");
								}
								queryResults.add(result);
							}
						}
						acc = effectiveness(queryResults);
						System.out.println("  new accuracy = " + acc);
						if (acc < prevAcc) {
							break;
						}
						prevAcc = acc;
					}
				}
			}
		}
	}

	public static double effectiveness(List<QueryResult> queryResults) {
		double acc = 0;
		for (QueryResult qr : queryResults) {
			acc += qr.mrr();
		}
		acc /= queryResults.size();
		return acc;
	}

}
