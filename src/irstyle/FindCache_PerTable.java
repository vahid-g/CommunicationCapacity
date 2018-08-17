package irstyle;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.lucene.store.RAMDirectory;

import database.DatabaseConnection;
import database.DatabaseType;
import irstyle.core.JDBCaccess;
import irstyle.core.Relation;
import irstyle.core.Schema;
import query.ExperimentQuery;
import query.QueryServices;

public class FindCache_PerTable {

	public static void main(String[] args) throws Exception {
		Params.MAX_TS_SIZE = 100;
		List<String> argList = Arrays.asList(args);
		List<ExperimentQuery> queries = null;
		if (argList.contains("-inex")) {
			queries = QueryServices.loadInexQueries();
		} else {
			queries = QueryServices.loadMsnQueries();
		}
		if (argList.contains("-debug")) {
			Params.DEBUG = true;
		}
		Collections.shuffle(queries, new Random(1));
		queries = queries.subList(0, 10);
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			String[] tableNames = ExperimentConstants.tableName;
			Connection conn = dc.getConnection();
			String[] cacheTables = new String[tableNames.length];
			String[] selectTemplates = new String[tableNames.length];
			String[] insertTemplates = new String[tableNames.length];
			String[] indexPaths = new String[tableNames.length];
			RAMDirectory[] ramDir = new RAMDirectory[tableNames.length];
			int[] pageSize = new int[tableNames.length];
			for (int i = 0; i < pageSize.length; i++) {
				pageSize[i] = ExperimentConstants.size[i] / 10;
			}
			IndexWriterConfig[] config = new IndexWriterConfig[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				cacheTables[i] = "tmp_" + tableNames[i].substring(4);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("drop table if exists " + cacheTables[i] + ";");
					stmt.execute(
							"create table " + cacheTables[i] + " as select id from " + tableNames[i] + " limit 0;");
					stmt.execute("create index id on " + cacheTables[i] + "(id);");
				}
				selectTemplates[i] = "select * from " + tableNames[i] + " order by popularity desc limit ?, "
						+ pageSize[i] + ";";
				insertTemplates[i] = "insert into " + cacheTables[i] + " (id) values (?);";
				indexPaths[i] = ExperimentConstants.MAPLE_DATA_DIR + cacheTables[i];
				config[i] = new IndexWriterConfig(new StandardAnalyzer());
				config[i].setSimilarity(new BM25Similarity());
				config[i].setRAMBufferSizeMB(1024);
				config[i].setOpenMode(OpenMode.CREATE);
			}
			IndexWriter indexWriters[] = new IndexWriter[tableNames.length];
			PreparedStatement selectSt[] = new PreparedStatement[tableNames.length];
			PreparedStatement insertSt[] = new PreparedStatement[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				ramDir[i] = new RAMDirectory();
				indexWriters[i] = new IndexWriter(ramDir[i], config[i]);
				indexWriters[i].commit();
				selectSt[i] = conn.prepareStatement(selectTemplates[i]);
				insertSt[i] = conn.prepareStatement(insertTemplates[i]);
			}
			double prevAcc = 0;
			double acc = 0;
			double bestAcc = 0;
			int[] offset = { 0, 0, 0 };
			int loop = 1;
			JDBCaccess jdbcacc = IRStyleKeywordSearch.jdbcAccess();
			IndexReader articleReader = DirectoryReader
					.open(FSDirectory.open(Paths.get(ExperimentConstants.MAPLE_DATA_DIR + "tbl_article_wiki13/100")));
			IndexReader imageReader = DirectoryReader
					.open(FSDirectory.open(Paths.get(ExperimentConstants.MAPLE_DATA_DIR + "tbl_image_pop/100")));
			IndexReader linkReader = DirectoryReader
					.open(FSDirectory.open(Paths.get(ExperimentConstants.MAPLE_DATA_DIR + "tbl_link_pop/100")));
			IndexReader[] indexReader = new IndexReader[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				System.out.println("================================");
				System.out.println("processing table: " + tableNames[i]);
				indexReader[0] = articleReader;
				indexReader[1] = imageReader;
				indexReader[2] = linkReader;

				String usedTable[] = new String[tableNames.length];
				usedTable[0] = tableNames[0];
				usedTable[1] = tableNames[1];
				usedTable[2] = tableNames[2];
				usedTable[i] = cacheTables[i];
				String articleImageTable = "tbl_article_image_09";
				String articleLinkTable = "tbl_article_link_09";
				String schemaDescription = "5 " + usedTable[0] + " " + articleImageTable + " " + usedTable[1] + " "
						+ articleLinkTable + " " + usedTable[2] + " " + usedTable[0] + " " + articleImageTable + " "
						+ articleImageTable + " " + usedTable[1] + " " + usedTable[0] + " " + articleLinkTable + " "
						+ articleLinkTable + " " + usedTable[2];
				Vector<Relation> relations = IRStyleKeywordSearch.createRelations(usedTable[0], usedTable[1],
						usedTable[2], articleImageTable, articleLinkTable, jdbcacc.conn);
				IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
				while (true) {
					System.out.println("Iteration " + loop++);
					if (offset[i] + pageSize[i] > ExperimentConstants.size[i]) {
						System.out.println("Scanned all table");
						break;
					}
					selectSt[i].setInt(1, offset[i]);
					offset[i] += pageSize[i];
					ResultSet rs = selectSt[i].executeQuery();
					List<Document> docs = new ArrayList<Document>();
					System.out.println("  reading new cache data..");
					while (rs.next()) {
						int id = rs.getInt("id");
						String text = "";
						for (String attrib : ExperimentConstants.textAttribs[i]) {
							text += rs.getString(attrib);
						}
						Document doc = new Document();
						doc.add(new StoredField("id", id));
						doc.add(new TextField("text", text, Store.NO));
						docs.add(doc);
					}
					System.out.println("  updating cache table..");
					for (Document doc : docs) {
						insertSt[i].setInt(1, Integer.parseInt(doc.get("id")));
						insertSt[i].addBatch();
					}
					insertSt[i].executeBatch();
					System.out.println("  updating cache index..");
					indexWriters[i].addDocuments(docs);
					indexWriters[i].flush();
					indexWriters[i].commit();
					System.out.println("  testing new cache..");
					List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
					indexReader[i] = DirectoryReader.open(ramDir[i]);
					System.out.println("  index sizes: " + indexReader[0].numDocs() + "," + indexReader[1].numDocs()
							+ "," + indexReader[2].numDocs());
					for (ExperimentQuery query : queries) {
						Schema sch = new Schema(schemaDescription);
						List<String> articleIds = RunCacheSearch.executeLuceneQuery(indexReader[0], query.getText());
						List<String> imageIds = RunCacheSearch.executeLuceneQuery(indexReader[1], query.getText());
						List<String> linkIds = RunCacheSearch.executeLuceneQuery(indexReader[2], query.getText());
						Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
						relnamesValues.put(usedTable[0], articleIds);
						relnamesValues.put(usedTable[1], imageIds);
						relnamesValues.put(usedTable[2], linkIds);
						IRStyleQueryResult result = RunCacheSearch.executeIRStyleQuery(jdbcacc, sch, relations, query,
								relnamesValues);
						queryResults.add(result);
					}
					indexReader[i].close();
					acc = effectiveness(queryResults);
					System.out.println("  new accuracy = " + acc);
					System.out.println("  current offsets: " + offset[i]);
					if (acc > bestAcc) {
						bestAcc = acc;
					}
					if ((prevAcc - bestAcc) > 0.005) {
						break;
					}
					prevAcc = acc;
				}
			}
			articleReader.close();
			imageReader.close();
			linkReader.close();
		}
	}

	public static double effectiveness(List<IRStyleQueryResult> queryResults) {
		double acc = 0;
		for (IRStyleQueryResult qr : queryResults) {
			acc += qr.p20();
		}
		acc /= queryResults.size();
		return acc;
	}

}
