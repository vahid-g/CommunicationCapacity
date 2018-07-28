package irstyle;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;
import irstyle_core.JDBCaccess;
import irstyle_core.Relation;
import irstyle_core.Schema;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiFilesPaths;

public class RunBuildCache {

	public static void main(String[] args) throws Exception {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			String[] tableNames = new String[] { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
			String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
			Connection conn = dc.getConnection();
			String[] cacheTables = new String[tableNames.length];
			String[] selectTemplates = new String[tableNames.length];
			String[] insertTemplates = new String[tableNames.length];
			String[] indexPaths = new String[tableNames.length];
			int pageSize = 100000;
			IndexWriterConfig[] config = new IndexWriterConfig[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				cacheTables[i] = "sub_" + tableNames[i].substring(4);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("drop table if exists " + cacheTables[i] + ";");
					stmt.execute(
							"create table " + cacheTables[i] + " as select id from " + tableNames[i] + " limit 0;");
					stmt.execute("create index id on " + cacheTables[i] + "(id);");
				}
				selectTemplates[i] = "select * from " + tableNames[i] + " order by popularity desc limit ?, " + pageSize
						+ ";";
				insertTemplates[i] = "insert into " + cacheTables[i] + " (id) values (?);";
				indexPaths[i] = "/data/ghadakcv/wikipedia/" + cacheTables[i];
				config[i] = new IndexWriterConfig(new StandardAnalyzer());
				config[i].setSimilarity(new BM25Similarity());
				config[i].setRAMBufferSizeMB(1024);
				config[i].setOpenMode(OpenMode.CREATE);
			}
			WikiFilesPaths paths = WikiFilesPaths.getMaplePaths();
			List<ExperimentQuery> queries = null;
			queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(), paths.getMsnQrelFilePath());
			queries = queries.subList(0, 50);
			int topDocs = 100;
			IndexWriter indexWriters[] = new IndexWriter[tableNames.length];
			PreparedStatement selectSt[] = new PreparedStatement[tableNames.length];
			PreparedStatement insertSt[] = new PreparedStatement[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				indexWriters[i] = new IndexWriter(FSDirectory.open(Paths.get(indexPaths[i])), config[i]);
				indexWriters[i].commit();
				selectSt[i] = conn.prepareStatement(selectTemplates[i]);
				insertSt[i] = conn.prepareStatement(insertTemplates[i]);
			}
			double prevAcc = 0;
			double acc = 0;
			int[] offset = { 0, 0, 0 };
			int loop = 1;
			JDBCaccess jdbcacc = IRStyleKeywordSearch.jdbcAccess();
			String articleTable = cacheTables[0];
			String imageTable = cacheTables[1];
			String linkTable = cacheTables[2];
			String articleImageTable = "tbl_article_image_09";
			String articleLinkTable = "tbl_article_link_09";
			String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
					+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
					+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
					+ articleLinkTable + " " + linkTable;
			Vector<Relation> relations = IRStyleKeywordSearch.createRelations(articleTable, imageTable, linkTable,
					jdbcacc.conn);
			IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
			List<List<Document>> docsList = new ArrayList<List<Document>>();
			int[] lastPopularity = new int[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				selectSt[i].setInt(1, offset[i]);
				offset[i] += pageSize;
				ResultSet rs = selectSt[i].executeQuery();
				List<Document> docs = new ArrayList<Document>();
				while (rs.next()) {
					int id = rs.getInt("id");
					String text = "";
					for (String attrib : textAttribs[i]) {
						text += rs.getString(attrib);
					}
					Document doc = new Document();
					doc.add(new StoredField("id", id));
					doc.add(new TextField("text", text, Store.NO));
					docs.add(doc);
					lastPopularity[i] = rs.getInt("popularity");
				}
				docsList.add(docs);
			}
			while (true) {
				int mPopularity = 0;
				int m = -1;
				for (int i = 0; i < lastPopularity.length; i++) {
					if (lastPopularity[i] > mPopularity) {
						mPopularity = lastPopularity[i];
						m = i;
					}
				}
				System.out.println("Iteration " + loop++);
				List<Document> docs = docsList.get(m);
				System.out.println("  reading new cache data..");
				for (Document doc : docs) {
					insertSt[m].setInt(1, Integer.parseInt(doc.get("id")));
					insertSt[m].addBatch();
				}
				System.out.println("  updating cache table..");
				insertSt[m].executeBatch();
				System.out.println("  updating cache index..");
				indexWriters[m].addDocuments(docs);
				indexWriters[m].flush();
				indexWriters[m].commit();
				// test partition!
				System.out.println("  testing new cache..");
				List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
				try (IndexReader articleReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPaths[0])));
						IndexReader imageReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPaths[1])));
						IndexReader linkReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPaths[2])))) {
					for (ExperimentQuery query : queries) {
						Schema sch = new Schema(schemaDescription);
						List<String> articleIds = RunBaselineWithLucene.executeLuceneQuery(articleReader,
								query.getText());
						List<String> imageIds = RunBaselineWithLucene.executeLuceneQuery(imageReader, query.getText());
						List<String> linkIds = RunBaselineWithLucene.executeLuceneQuery(linkReader, query.getText());
						//System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d", articleIds.size(),
								// imageIds.size(), linkIds.size());
						Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
						relnamesValues.put(articleTable, articleIds);
						relnamesValues.put(imageTable, imageIds);
						relnamesValues.put(linkTable, linkIds);
						IRStyleQueryResult result = RunBaselineWithLucene.executeIRStyleQuery(jdbcacc, sch, relations,
								query, relnamesValues);
						queryResults.add(result);
					}

					// DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
					// System.out.println(" index size: " + reader.numDocs());
					// IndexSearcher searcher = new IndexSearcher(reader);
					// QueryParser qp = new QueryParser("text", new StandardAnalyzer());
					// for (ExperimentQuery q : queries) {
					// QueryResult result = new QueryResult(q);
					// Query query = qp.parse(QueryParser.escape(q.getText()));
					// ScoreDoc[] scoreDocHits = searcher.search(query, topDocs).scoreDocs;
					// for (int j = 0; j < Math.min(topDocs, scoreDocHits.length); j++) {
					// Document doc = reader.document(scoreDocHits[j].doc);
					// String docId = doc.get("id");
					// result.addResult(docId, "no title");
					// }
					// queryResults.add(result);
					// }
					// }
					acc = effectiveness2(queryResults);
					System.out.println("  new accuracy = " + acc);

				}
				if (acc < prevAcc) {
					break;
				}
				prevAcc = acc;

				// update buffer
				selectSt[m].setInt(1, offset[m]);
				offset[m] += pageSize;
				ResultSet rs = selectSt[m].executeQuery();
				while (rs.next()) {
					int id = rs.getInt("id");
					String text = "";
					for (String attrib : textAttribs[m]) {
						text += rs.getString(attrib);
					}
					Document doc = new Document();
					doc.add(new StoredField("id", id));
					doc.add(new TextField("text", text, Store.NO));
					docs.add(doc);
					lastPopularity[m] = rs.getInt("popularity");
				}
				docsList.remove(m);
				docsList.add(m, docs);
			}
			for (int i = 0; i < tableNames.length; i++) {
				indexWriters[i].close();
				selectSt[i].close();
				insertSt[i].close();

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

	public static double effectiveness2(List<IRStyleQueryResult> queryResults) {
		double acc = 0;
		for (IRStyleQueryResult qr : queryResults) {
			acc += qr.rrank();
		}
		acc /= queryResults.size();
		return acc;
	}

}
