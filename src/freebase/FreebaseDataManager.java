package freebase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.jcraft.jsch.Session;

public class FreebaseDataManager {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static String DB_URL = "jdbc:mysql://engr-db.engr.oregonstate.edu:3307/querycapacity";
	// DB_URL = "jdbc:mysql://localhost:4321/querycapacity";
	static final int MAX_HITS = 100;
	static final int MAX_FETCH = 1000;
	static final String USER = "querycapacity";
	static final String PASS = "";
	static final String NAME_ATTRIB = "name";
	static final String DESC_ATTRIB = "description";
	static final String FBID_ATTRIB = "fbid";
	static final String PROF_ATTRIB = "profession";
	static final String GENRE_ATTRIB = "genre";
	static final String FREQ_ATTRIB = "frequency";
	static final String SEMANTIC_TYPE_ATTRIB = "semantic_type";
	public static Session session;

	static final Logger LOGGER = Logger.getLogger(FreebaseDataManager.class.getName());
	static {
		LOGGER.setUseParentHandlers(false);
		Handler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		LOGGER.addHandler(handler);
		LOGGER.setLevel(Level.ALL);
	}

	static Connection getDatabaseConnection() throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", FreebaseDataManager.USER);
		connectionProps.put("password", FreebaseDataManager.PASS);
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(FreebaseDataManager.DB_URL, connectionProps);
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		}
		return conn;
	}

	public static String buildDataQuery(String tableName, String[] attribs) {
		StringBuilder sb = new StringBuilder();
		sb.append("select fbid");
		for (String attrib : attribs) {
			sb.append(", " + attrib);
		}
		sb.append(" from " + tableName);
		String sql = sb.toString();
		LOGGER.log(Level.INFO, "Data query: " + sql);
		return sql;
	}

	public static String buildConditionalDataQuery(String tableName, String[] attribs, int lo, int hi) {
		String baseQuery = buildDataQuery(tableName, attribs);
		String sql = baseQuery + " where id > " + lo + " AND id < " + hi;
		return sql;
	}

	// default value for fetch size
	public static List<Document> loadTuplesToDocuments(String sqlQuery, String[] attribs) {
		return loadTuplesToDocuments(sqlQuery, attribs, Integer.MIN_VALUE);
	}

	public static List<Document> loadTuplesToDocuments(String sqlQuery, String[] attribs, int fetchSize) {
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList<Document> docList = new ArrayList<Document>();
		try (Connection databaseConnection = getDatabaseConnection()) {
			// retrieve the tuples to be indexed
			stmt = databaseConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(fetchSize);
			rs = stmt.executeQuery(sqlQuery);
			Runtime runtime = Runtime.getRuntime();
			LOGGER.log(Level.INFO, "Starting to retrieve data. Memory Used: "
					+ (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024) + " MB");
			while (rs.next()) {
				Document doc = new Document();
				try {
					doc.add(new StoredField(FreebaseDataManager.FBID_ATTRIB,
							rs.getString(FreebaseDataManager.FBID_ATTRIB)));
					for (String attrib : attribs) {
						doc.add(new TextField(attrib, rs.getString(attrib), Field.Store.YES));
					}
				} catch (IllegalArgumentException e) { // the field is null
					// do nothing
				}
				docList.add(doc);
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, e.toString());
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
		}
		return docList;
	}

	public static List<Document> loadTuplesToDocumentsSmoothing(String sqlQuery, String[] attribs, int fetchSize,
			Map<String, Float> weights, double alpha, int tbl_size) {
		int N = 0;
		for (float n_i : weights.values()) {
			N += n_i;
		}
		int V = tbl_size;
		Map<String, Float> smoothWeights = new HashMap<String, Float>();
		for (String key : weights.keySet()) {
			float w = weights.get(key);
			float smoothed = (float) ((w + alpha) / (N + V * alpha));
			smoothWeights.put(key, smoothed);
		}
		float smoothed = (float) (alpha / (V * alpha));
		return loadTuplesToDocuments(sqlQuery, attribs, fetchSize, smoothWeights, smoothed);
	}

	@SuppressWarnings("deprecation")
	public static List<Document> loadTuplesToDocuments(String sqlQuery, String[] attribs, int fetchSize,
			Map<String, Float> weights, float smooth) {
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList<Document> docList = new ArrayList<Document>();
		try (Connection databaseConnection = getDatabaseConnection()) {
			// retrieve the tuples to be indexed
			stmt = databaseConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(fetchSize);
			rs = stmt.executeQuery(sqlQuery);
			Runtime runtime = Runtime.getRuntime();
			LOGGER.log(Level.INFO, "Starting to retrieve data. Memory Used: "
					+ (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024) + " MB");
			while (rs.next()) {
				Document doc = new Document();
				try {
					String fbid = rs.getString(FBID_ATTRIB);
					doc.add(new StoredField(FreebaseDataManager.FBID_ATTRIB, fbid));
					float weight = smooth;
					if (weights.containsKey(fbid)) {
						weight = weights.get(fbid);
					}
					doc.add(new StoredField(FREQ_ATTRIB, weight));
					for (String attrib : attribs) {
						TextField tf = new TextField(attrib, rs.getString(attrib), Field.Store.YES);
						tf.setBoost(weight);
						doc.add(tf);
					}
				} catch (IllegalArgumentException e) { // the field is null
					// do nothing
				}
				if (doc.get(FBID_ATTRIB) == null)
					LOGGER.log(Level.INFO, "hanhan");
				docList.add(doc);
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				LOGGER.log(Level.SEVERE, e.toString());
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
		}
		return docList;
	}

	// loads fbid -> weight into a hashmap from list of queries;
	public static TreeMap<String, Float> loadQueryWeights(List<FreebaseQuery> queries) {
		TreeMap<String, Float> weightMap = new TreeMap<String, Float>();
		for (FreebaseQuery query : queries) {
			String fbid = query.fbid;
			if (weightMap.containsKey(fbid)) {
				weightMap.put(fbid, weightMap.get(fbid) + query.frequency);
			} else {
				weightMap.put(fbid, (float) query.frequency);
			}
		}
		return weightMap;
	}

	public static TreeMap<String, Float> loadQueryInstanceWeights(List<FreebaseQuery> queries) {
		TreeMap<String, Float> weightMap = new TreeMap<String, Float>();
		for (FreebaseQuery query : queries) {
			String fbid = query.fbid;
			if (weightMap.containsKey(fbid)) {
				weightMap.put(fbid, weightMap.get(fbid) + 1.0f);
			} else {
				weightMap.put(fbid, 1.0f);
			}
		}
		return weightMap;
	}

	// default value for docs size (runs on all docs)
	public static String createIndex(List<Document> docs, String[] attribs, String indexPath) {
		return createIndex(docs, docs.size(), attribs, indexPath);
	}

	public static String createIndex(List<Document> docs, int docCount, String[] attribs, String indexPath) {
		return createAppendIndex(docs, docCount, attribs, indexPath, false);
	}

	public static String appendIndex(List<Document> docs, int docCount, String[] attribs, String indexPath) {
		return createAppendIndex(docs, docCount, attribs, indexPath, true);
	}

	// this method builds an index on lucene Documents array. This method just
	// processes the first docCount number of docs from the array
	public static String createAppendIndex(List<Document> docs, int docCount, String[] attribs, String indexPath,
			boolean append) {
		LOGGER.log(Level.INFO, "Creating Index...");
		if (!append)
			cleanupIndexDir(indexPath);
		Directory directory = null;
		IndexWriter writer = null;
		try {
			// creating the index
			directory = FSDirectory.open(Paths.get(indexPath));
			IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
			config.setRAMBufferSizeMB(1024.0);
			if (append)
				config.setOpenMode(OpenMode.APPEND);
			else
				config.setOpenMode(OpenMode.CREATE);

			writer = new IndexWriter(directory, config);
			for (int i = 0; i < docCount; i++) {
				Document doc = docs.get(i);
				try {
					writer.addDocument(doc);
				} catch (IllegalArgumentException e) { // the field is null
					// do nothing
				}
			}
			LOGGER.log(Level.INFO, "Indexed documents: " + docCount);
		} catch (Exception e) {
			// LOGGER.log(Level.SEVERE, e.toString());
			e.printStackTrace();
		} finally {
			try {
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				// LOGGER.log(Level.SEVERE, e.toString());
				e.printStackTrace();
			}
			try {
				if (directory != null)
					directory.close();
			} catch (IOException e) {
				// LOGGER.log(Level.SEVERE, e.toString());
				e.printStackTrace();
			}
		}
		return indexPath;
	}

	private static void cleanupIndexDir(String indexPath) {
		File dirFile = new File(indexPath);
		if (dirFile.exists()) {
			for (File f : dirFile.listFiles()) {
				f.delete();
			}
		} else {
			dirFile.mkdir();
		}
	}

	public static List<FreebaseQuery> loadMsnQueries(String queryTable) {
		String sql = "select * from " + queryTable + ";";
		return FreebaseDataManager.loadMsnQueriesFromSql(sql);
	}

	public static List<FreebaseQuery> loadMsnQueriesWithMaxMrr(String queryTable, double maxMrr) {
		String sql = "select * from " + queryTable + " where mrr < " + maxMrr + ";";
		return FreebaseDataManager.loadMsnQueriesFromSql(sql);
	}

	public static List<FreebaseQuery> loadMsnQueriesFromSql(String sqlQuery) {
		List<FreebaseQuery> queries = new ArrayList<FreebaseQuery>();
		Statement st = null;
		ResultSet rs = null;
		try (Connection conn = getDatabaseConnection()) {
			st = conn.createStatement();
			rs = st.executeQuery(sqlQuery);
			while (rs.next()) {
				HashMap<String, String> attribs = new HashMap<String, String>();
				attribs.put(NAME_ATTRIB, rs.getString("text"));
				attribs.put(DESC_ATTRIB, rs.getString("text"));
				FreebaseQuery q = new FreebaseQuery(rs.getInt("id"), attribs);
				q.text = rs.getString("text").trim().replace(",", " ");
				q.wiki = rs.getString("wiki_id");
				q.frequency = rs.getInt(FreebaseDataManager.FREQ_ATTRIB);
				q.fbid = rs.getString(FreebaseDataManager.FBID_ATTRIB);
				queries.add(q);
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e2) {
				LOGGER.log(Level.SEVERE, e2.toString());
			}
			try {
				if (st != null)
					st.close();
			} catch (SQLException e2) {
				LOGGER.log(Level.SEVERE, e2.toString());
			}
		}
		return queries;
	}

	public static List<FreebaseQuery> loadMsnQueriesByRelevantTable(String tableName) {
		LOGGER.log(Level.INFO, "Building data query");
		String sqlQuery = "select id, wiki_id, fbid, text, frequency from tbl_query as q where q.fbid in (select fbid from "
				+ tableName + ")";
		return loadMsnQueriesFromSql(sqlQuery);
	}

	public static void removeKeyword(List<FreebaseQuery> queries, String pattern) {
		for (FreebaseQuery query : queries) {
			extractKeyword(query, pattern);
		}
	}

	public static void annotateSemanticType(List<FreebaseQuery> queries, String pattern) {
		for (FreebaseQuery query : queries) {
			String keyword = extractKeyword(query, pattern);
			query.attribs.put(FreebaseDataManager.SEMANTIC_TYPE_ATTRIB, keyword);
		}
	}

	// removes a keyword (based on provided pattern) from a queries name and
	// description and returns it
	private static String extractKeyword(FreebaseQuery query, String pattern) {
		Pattern pat = Pattern.compile(pattern);
		Matcher matcher = pat.matcher(query.text.toLowerCase());
		matcher.find();
		String keyword = matcher.group(0);
		query.attribs.put(FreebaseDataManager.NAME_ATTRIB, query.text.toLowerCase().replace(keyword, ""));
		query.attribs.put(DESC_ATTRIB, query.text.toLowerCase().replace(keyword, ""));
		return keyword;
	}

	public static void addIdenticalAttibuteToQueries(List<FreebaseQuery> list, String attrib, String val) {
		for (FreebaseQuery query : list) {
			query.attribs.put(attrib, val);
		}
	}

	// same code as above but returns an object of FreebaseQueryResult class
	public static FreebaseQueryResult runFreebaseQuery(FreebaseQuery freebaseQuery, String indexPath) {
		IndexReader reader = null;
		FreebaseQueryResult fqr = new FreebaseQueryResult(freebaseQuery);
		try {
			reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
			for (String attrib : freebaseQuery.attribs.keySet()) {
				builder.add(new QueryParser(attrib, new StandardAnalyzer())
						.parse(QueryParser.escape(freebaseQuery.attribs.get(attrib))), BooleanClause.Occur.SHOULD);
			}
			Query query = builder.build();
			// LOGGER.log(Level.INFO, "submitting query: " + query.toString());
			TopDocs topDocs = searcher.search(query, FreebaseDataManager.MAX_HITS);
			// LOGGER.log(Level.INFO, "hits length: " + hits.length);
			for (int i = 0; i < topDocs.scoreDocs.length; i++) {
				Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
				if (doc.get(FreebaseDataManager.FBID_ATTRIB).equals(freebaseQuery.fbid)) {
					fqr.relRank = i + 1;
					break;
				}
			}
			int precisionBoundry = topDocs.scoreDocs.length > 3 ? 3 : topDocs.scoreDocs.length;
			for (int i = 0; i < precisionBoundry; i++) {
				Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
				fqr.top3Hits[i] = doc.get(FreebaseDataManager.NAME_ATTRIB);
			}

		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.toString());
			}
		}
		return fqr;
	}

	// same as above but runs more than one query
	public static List<FreebaseQueryResult> runFreebaseQueries(List<FreebaseQuery> queries, String indexPath) {
		LOGGER.log(Level.INFO, "Running " + queries.size() + " queries on " + indexPath);
		IndexReader reader = null;
		List<FreebaseQueryResult> fqrList = new ArrayList<FreebaseQueryResult>();
		try {
			reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
			LOGGER.log(Level.INFO, "Loaded docs: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			for (FreebaseQuery freebaseQuery : queries) {
				FreebaseQueryResult fqr = new FreebaseQueryResult(freebaseQuery);
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				for (String attrib : freebaseQuery.attribs.keySet()) {
					builder.add(new QueryParser(attrib, new StandardAnalyzer())
							.parse(QueryParser.escape(freebaseQuery.attribs.get(attrib))), BooleanClause.Occur.SHOULD);
				}
				Query query = builder.build();
				// LOGGER.log(Level.INFO, "submitting query: " +
				// query.toString());
				TopDocs topDocs = searcher.search(query, FreebaseDataManager.MAX_HITS);
				for (int j = 0; j < topDocs.scoreDocs.length; j++) {
					Document doc = searcher.doc(topDocs.scoreDocs[j].doc);
					if (j < 3) {
						fqr.top3Hits[j] = doc.get(FreebaseDataManager.NAME_ATTRIB);
					}
					if (doc.get(FreebaseDataManager.FBID_ATTRIB).equals(freebaseQuery.fbid)) {
						// System.out.println(searcher.explain(query,
						// hits[i].doc));
						fqr.relRank = j + 1;
						break;
					}
				}
				fqrList.add(fqr);
			}

		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString());
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.toString());
			}
		}
		return fqrList;
	}

	// conver list of results to map, this is useful to aggregate results of
	// multiple runs
	public static Map<FreebaseQuery, FreebaseQueryResult> convertResultListToMap(List<FreebaseQueryResult> resultList) {
		Map<FreebaseQuery, FreebaseQueryResult> resultMap = new HashMap<FreebaseQuery, FreebaseQueryResult>();
		for (FreebaseQueryResult fqr : resultList) {
			resultMap.put(fqr.freebaseQuery, fqr);
		}
		return resultMap;
	}

}
