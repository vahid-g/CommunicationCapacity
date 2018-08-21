package irstyle.old;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;
import irstyle.ExperimentConstants;
import irstyle.IRStyleKeywordSearch;
import irstyle.IRStyleQueryResult;
import irstyle.RunCacheSearch;
import irstyle.api.Params;
import irstyle.core.JDBCaccess;
import irstyle.core.Relation;
import irstyle.core.Schema;
import query.ExperimentQuery;
import query.QueryServices;

public class FindCache_InefficienMethod {

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
			String[] tableNames = ExperimentConstants.tableName;
			String[] shortTableNames = new String[] { "a", "i", "l" };
			String[][] textAttribs = ExperimentConstants.textAttribs;
			Connection conn = dc.getConnection();
			conn.setAutoCommit(false);
			String[] cacheTableNames = new String[tableNames.length];
			String[] currentTupleSelectSql = new String[tableNames.length];
			String[] insertTemplates = new String[tableNames.length];
			double[] sizes = { 11945034, 1183070, 9766351 };
			String[] joinMaxSelectTemplate = new String[tableNames.length];
			String[] joinSelectTemplate = new String[tableNames.length];
			String from = " from tbl_article_wiki13 a left join tbl_article_image_09 ai on a.id = ai.article_id "
					+ "left join tbl_image_pop i on ai.image_id = i.id "
					+ "left join tbl_article_link_09 al on a.id = al.article_id "
					+ "left join tbl_link_pop l on al.link_id = l.id";
			String whereTemplate = " where ID_PLACE_HOLDER = ?";
			String max = " max(least(a.popularity, ifnull(i.popularity, a.popularity),"
					+ "ifnull(l.popularity, a.popularity))) m";
			// String max = " least(a.popularity, ifnull(i.popularity, a.popularity),
			// ifnull(l.popularity, a.popularity)) m";
			String groupBy = " group by ID_PLACE_HOLDER ;";
			String joinMaxSelectPrefix = "select ID_PLACE_HOLDER," + max + from + whereTemplate + groupBy;
			String joinSelectPrefix = "select a.id, a.title, a.text, i.id, i.src, l.id, l.url" + from + whereTemplate;
			PreparedStatement selectSt[] = new PreparedStatement[tableNames.length];
			PreparedStatement insertSt[] = new PreparedStatement[tableNames.length];
			PreparedStatement joinMaxSelectSt[] = new PreparedStatement[tableNames.length];
			PreparedStatement joinSelectSt[] = new PreparedStatement[tableNames.length];
			// RAMDirectory[] indexDir = new RAMDirectory[tableNames.length];
			Directory[] indexDir = new Directory[tableNames.length];
			IndexWriterConfig[] config = new IndexWriterConfig[tableNames.length];
			IndexWriter indexWriters[] = new IndexWriter[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				// prepare cache tables
				cacheTableNames[i] = "tmp_" + tableNames[i].substring(4);
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("drop table if exists " + cacheTableNames[i] + ";");
					stmt.execute(
							"create table " + cacheTableNames[i] + " as select id from " + tableNames[i] + " limit 0;");
					stmt.execute("create index id on " + cacheTableNames[i] + "(id);");
				}
				// prepare prepared statements
				// TODO: rename popularity column
				currentTupleSelectSql[i] = "select id from " + tableNames[i] + " order by popularity desc;";
				insertTemplates[i] = "insert into " + cacheTableNames[i] + " (id) values (?);";
				joinMaxSelectTemplate[i] = joinMaxSelectPrefix.replaceAll("ID_PLACE_HOLDER",
						shortTableNames[i] + ".id");
				joinSelectTemplate[i] = joinSelectPrefix.replaceAll("ID_PLACE_HOLDER", shortTableNames[i] + ".id");
				selectSt[i] = conn.prepareStatement(currentTupleSelectSql[i]);
				insertSt[i] = conn.prepareStatement(insertTemplates[i]);
				joinMaxSelectSt[i] = conn.prepareStatement(joinMaxSelectTemplate[i]);
				joinSelectSt[i] = conn.prepareStatement(joinSelectTemplate[i]);
				// prepare index writer
				config[i] = new IndexWriterConfig(new StandardAnalyzer());
				config[i].setSimilarity(new BM25Similarity());
				config[i].setRAMBufferSizeMB(1024);
				config[i].setOpenMode(OpenMode.CREATE);
				// indexDir[i] = new RAMDirectory();
				indexDir[i] = FSDirectory.open(Paths.get("/data/ghadakcv/wikipedia/" + cacheTableNames[i]));
				indexWriters[i] = new IndexWriter(indexDir[i], config[i]);
				indexWriters[i].commit();
			}
			double prevAcc = 0;
			double acc = 0;
			double bestAcc = 0;
			int loop = 1;
			JDBCaccess jdbcacc = IRStyleKeywordSearch.jdbcAccess();
			String articleTable = cacheTableNames[0];
			String imageTable = cacheTableNames[1];
			String linkTable = cacheTableNames[2];
			String articleImageTable = "tbl_article_image_09";
			String articleLinkTable = "tbl_article_link_09";
			String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
					+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
					+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
					+ articleLinkTable + " " + linkTable;
			Vector<Relation> relations = IRStyleKeywordSearch.createRelations(articleTable, imageTable, linkTable,
					articleImageTable, articleLinkTable, jdbcacc.conn);
			IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
			int[] currentMaxPopularity = new int[tableNames.length];
			int[] currentTupleId = new int[tableNames.length];
			ResultSet[] currentTupleRS = new ResultSet[tableNames.length];
			List<Set<Integer>> cachedTuplesList = new ArrayList<Set<Integer>>();
			for (int i = 0; i < tableNames.length; i++) {
				cachedTuplesList.add(new HashSet<Integer>());
				currentTupleRS[i] = selectSt[i].executeQuery();
				currentTupleRS[i].next();
				currentTupleId[i] = currentTupleRS[i].getInt("id");
				joinMaxSelectSt[i].setInt(1, currentTupleId[i]);
				ResultSet maxPopularityRS = joinMaxSelectSt[i].executeQuery();
				maxPopularityRS.next();
				currentMaxPopularity[i] = maxPopularityRS.getInt("m");
			}
			while (true) {
				System.out.println("Iteration " + loop++);
				System.out.println("  picking max popularity..");
				double maxPopularity = 0;
				int m = -1;
				for (int i = 0; i < currentTupleId.length; i++) {
					if (currentMaxPopularity[i] > maxPopularity) {
						maxPopularity = currentMaxPopularity[i];
						m = i;
					}
				}
				System.out.println("  selected table = " + tableNames[m] + " with popularity = " + maxPopularity);
				joinSelectSt[m].setInt(1, currentTupleId[m]);
				ResultSet joinRS = joinSelectSt[m].executeQuery();
				List<List<Document>> docs = new ArrayList<List<Document>>();
				for (int i = 0; i < tableNames.length; i++) {
					docs.add(new ArrayList<Document>());
				}
				System.out.println("  updating caches..");
				while (joinRS.next()) {
					for (int i = 0; i < tableNames.length; i++) {
						Integer id = joinRS.getInt(shortTableNames[i] + ".id");
						if (!cachedTuplesList.get(i).contains(id)) {
							cachedTuplesList.get(i).add(id);
							insertSt[i].setInt(1, id);
							insertSt[i].addBatch();
							String text = "";
							for (String attrib : textAttribs[i]) {
								text += joinRS.getString(attrib);
							}
							Document doc = new Document();
							doc.add(new StoredField("id", id));
							doc.add(new TextField("text", text, Store.NO));
							docs.get(i).add(doc);
						}
					}
				}
				System.out.println(
						"  adding docs to index: " + docs.stream().map(l -> l.size() + " ").reduce("", String::concat));
				for (int i = 0; i < tableNames.length; i++) {
					insertSt[i].executeBatch();
					indexWriters[i].addDocuments(docs.get(i));
					indexWriters[i].flush();
					indexWriters[i].commit();
				}
				conn.commit();
				System.out.println("  sizes so far: "
						+ cachedTuplesList.stream().map(l -> l.size() + " ").reduce("", String::concat));
				// updating pointers
				currentTupleRS[m].next();
				currentTupleId[m] = currentTupleRS[m].getInt("id");
				joinMaxSelectSt[m].setInt(1, currentTupleId[m]);
				ResultSet maxPopularityRS = joinMaxSelectSt[m].executeQuery();
				maxPopularityRS.next();
				currentMaxPopularity[m] = maxPopularityRS.getInt("m");
				// test partition!
				if (loop % 10000 == 0) {
					System.out.println("  testing new cache..");
					List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
					try (IndexReader articleReader = DirectoryReader.open(indexDir[0]);
							IndexReader imageReader = DirectoryReader.open(indexDir[1]);
							IndexReader linkReader = DirectoryReader.open(indexDir[2])) {
						System.out.println("  index sizes: " + articleReader.numDocs() + "," + imageReader.numDocs()
								+ "," + linkReader.numDocs());
						for (ExperimentQuery query : queries) {
							Schema sch = new Schema(schemaDescription);
							List<String> articleIds = RunCacheSearch.executeLuceneQuery(articleReader, query.getText());
							List<String> imageIds = RunCacheSearch.executeLuceneQuery(imageReader, query.getText());
							List<String> linkIds = RunCacheSearch.executeLuceneQuery(linkReader, query.getText());
							Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
							relnamesValues.put(articleTable, articleIds);
							relnamesValues.put(imageTable, imageIds);
							relnamesValues.put(linkTable, linkIds);
							IRStyleQueryResult result = RunCacheSearch.executeIRStyleQuery(jdbcacc, sch, relations,
									query, relnamesValues);
							queryResults.add(result);
						}
						acc = effectiveness(queryResults);
						System.out.println("  new accuracy = " + acc);

					}
					if (acc > bestAcc) {
						bestAcc = acc;
					}
					if ((acc - prevAcc) > 0.05) {
						System.out.println("  time to break;");
						break;
					}
					prevAcc = acc;
					if (currentMaxPopularity[0] == -1 && currentMaxPopularity[1] == -1
							&& currentMaxPopularity[-2] == -1) {
						break;
					}
				}
			}
			double[] percent = new double[tableNames.length];
			for (int i = 0; i < cachedTuplesList.size(); i++) {
				percent[i] = cachedTuplesList.get(i).size() / sizes[i];
			}
			System.out.println("Best found sizes = " + Arrays.toString(percent));
			for (int i = 0; i < tableNames.length; i++) {
				indexWriters[i].close();
				selectSt[i].close();
				insertSt[i].close();
				joinMaxSelectSt[i].close();
				joinSelectSt[i].close();
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
