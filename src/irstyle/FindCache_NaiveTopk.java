package irstyle;

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
import org.apache.lucene.store.RAMDirectory;

import database.DatabaseConnection;
import database.DatabaseType;
import irstyle_core.JDBCaccess;
import irstyle_core.Relation;
import irstyle_core.Schema;
import query.ExperimentQuery;
import query.QueryServices;

public class FindCache_NaiveTopk {

	private static final double popSum[] = { 15440314886.0, 52716653460.0, 1407925741807.0 };

	public static void main(String[] args) throws Exception {
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
			String[] tableNames = new String[] { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };
			String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };
			Connection conn = dc.getConnection();
			String[] cacheTables = new String[tableNames.length];
			String[] selectTemplates = new String[tableNames.length];
			String[] insertTemplates = new String[tableNames.length];
			String[] indexPaths = new String[tableNames.length];
			RAMDirectory[] ramDir = new RAMDirectory[tableNames.length];
			int[] pageSize = { 119450, 11830, 97663 };
			// int[] pageSize = { 100000, 100000, 100000 };
			double[] sizes = { 11945034, 1183070, 9766351 };
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
				indexPaths[i] = "/data/ghadakcv/wikipedia/" + cacheTables[i];
				config[i] = new IndexWriterConfig(new StandardAnalyzer());
				config[i].setSimilarity(new BM25Similarity());
				config[i].setRAMBufferSizeMB(1024);
				config[i].setOpenMode(OpenMode.CREATE);
			}
			IndexWriter indexWriters[] = new IndexWriter[tableNames.length];
			PreparedStatement selectSt[] = new PreparedStatement[tableNames.length];
			PreparedStatement insertSt[] = new PreparedStatement[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				// indexWriters[i] = new IndexWriter(FSDirectory.open(Paths.get(indexPaths[i])),
				// config[i]);
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
			int[] bestOffset = { 0, 0, 0 };
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
					articleImageTable, articleLinkTable, jdbcacc.conn);
			IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
			List<List<Document>> docsList = new ArrayList<List<Document>>();
			int[] lastPopularity = new int[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				selectSt[i].setInt(1, offset[i]);
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
				System.out.println("Iteration " + loop++);
				double mPopularity = 0;
				int m = -1;
				for (int i = 0; i < lastPopularity.length; i++) {
					if (lastPopularity[i] / popSum[i] > mPopularity) {
						mPopularity = lastPopularity[i] / popSum[i];
						m = i;
					}
				}
				System.out.println("  Selected table = " + tableNames[m] + " with popularity = " + mPopularity);
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
				try (IndexReader articleReader = DirectoryReader.open(ramDir[0]);
						IndexReader imageReader = DirectoryReader.open(ramDir[1]);
						IndexReader linkReader = DirectoryReader.open(ramDir[2])) {
					System.out.println("  index sizes: " + articleReader.numDocs() + "," + imageReader.numDocs() + ","
							+ linkReader.numDocs());
					for (ExperimentQuery query : queries) {
						Schema sch = new Schema(schemaDescription);
						List<String> articleIds = RunBaseline_Lucene.executeLuceneQuery(articleReader, query.getText());
						List<String> imageIds = RunBaseline_Lucene.executeLuceneQuery(imageReader, query.getText());
						List<String> linkIds = RunBaseline_Lucene.executeLuceneQuery(linkReader, query.getText());
						Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
						relnamesValues.put(articleTable, articleIds);
						relnamesValues.put(imageTable, imageIds);
						relnamesValues.put(linkTable, linkIds);
						IRStyleQueryResult result = RunBaseline_Lucene.executeIRStyleQuery(jdbcacc, sch, relations,
								query, relnamesValues);
						queryResults.add(result);
					}
					acc = effectiveness(queryResults);
					System.out.println("  new accuracy = " + acc);

				}
				offset[m] += pageSize[m];
				System.out.println("  current offsets: " + Arrays.toString(offset));
				if (acc > bestAcc) {
					bestAcc = acc;
					bestOffset = offset.clone();
				}
				if ((acc - prevAcc) > 0.05) {
					System.out.println("  time to break;");
					break;
				}
				prevAcc = acc;
				if (lastPopularity[0] == -1 && lastPopularity[1] == -1 && lastPopularity[-2] == -1) {
					break;
				}
				// update buffer
				selectSt[m].setInt(1, offset[m]);
				ResultSet rs = selectSt[m].executeQuery();
				lastPopularity[m] = -1;
				docs = new ArrayList<Document>();
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
			System.out.println("Offsets for articles, images, links = " + Arrays.toString(bestOffset));
			double[] percent = new double[bestOffset.length];
			for (int i = 0; i < bestOffset.length; i++) {
				percent[i] = bestOffset[i] / sizes[i];
			}
			System.out.println("Best found sizes = " + Arrays.toString(percent));
			for (int i = 0; i < tableNames.length; i++) {
				indexWriters[i].close();
				selectSt[i].close();
				insertSt[i].close();
			}
		}
	}

	public static double effectiveness(List<IRStyleQueryResult> queryResults) {
		double acc = 0;
		for (IRStyleQueryResult qr : queryResults) {
			acc += qr.recall();
		}
		acc /= queryResults.size();
		return acc;
	}

}
