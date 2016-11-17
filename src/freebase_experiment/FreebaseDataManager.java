package freebase_experiment;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
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

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class FreebaseDataManager {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static String DB_URL = "jdbc:mysql://engr-db.engr.oregonstate.edu:3307/querycapacity";
	// DB_URL = "jdbc:mysql://localhost:4321/querycapacity";
	static final int MAX_HITS = 100;
	static final String USER = "querycapacity";
	static final String PASS = "13667v";
	static final String INDEX_BASE = "data/freebase_index/";
	static final String resultDir = "data/result/";
	static final String NAME_ATTRIB = "name";
	static final String DESC_ATTRIB = "description";
	static final String FBID_ATTRIB = "fbid";
	static final String PROF_ATTRIB = "profession";
	static final String GENRE_ATTRIB = "genre";
	static final String FREQ_ATTRIB = "frequency";
	static final String SEMANTIC_TYPE_ATTRIB = "semantic_type";
	public static Session session;
	
	private static final int MAX_FETCH_SIZE = 10000;
	
	static void sshConnect() {
		String user = "ghadakcv";
		String password = "Hanh@nolde?";
		String host = "flip.engr.oregonstate.edu";
		int port = 22;
		int localPort = 4321;
		String remoteHost = "engr-db.engr.oregonstate.edu";
		int remotePort = 3307;
		try {
			JSch jsch = new JSch();
			FreebaseDataManager.session = jsch.getSession(user, host, port);
			FreebaseDataManager.session.setPassword(password);
			FreebaseDataManager.session
					.setConfig("StrictHostKeyChecking", "no");
			System.out.println("Establishing Connection...");
			FreebaseDataManager.session.connect();
			System.out.println("SSH Connection established.");
			int assinged_port = FreebaseDataManager.session.setPortForwardingL(
					localPort, remoteHost, remotePort);
			System.out.println("localhost:" + assinged_port + " -> "
					+ remoteHost + ":" + remotePort);
			System.out.println("Port Forwarded");
		} catch (JSchException e) {
			e.printStackTrace();
		}
	}

	static void sshDisconnect() {
		FreebaseDataManager.session.disconnect();
		System.out.println("SSH Disconnected");
	}

	public static String sendCommand(String command) {
		StringBuilder outputBuffer = new StringBuilder();
		try {
			Channel channel = FreebaseDataManager.session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			InputStream commandOutput = channel.getInputStream();
			channel.connect();
			int readByte = commandOutput.read();
			while (readByte != 0xffffffff) {
				outputBuffer.append((char) readByte);
				readByte = commandOutput.read();
			}
			channel.disconnect();
		} catch (IOException ioX) {
			ioX.printStackTrace();
		} catch (JSchException jschX) {
			jschX.printStackTrace();
		}
		return outputBuffer.toString();
	}

	private static Connection getDatabaseConnection() throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", FreebaseDataManager.USER);
		connectionProps.put("password", FreebaseDataManager.PASS);
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(FreebaseDataManager.DB_URL,
					connectionProps);
		} catch (SQLException e) {
			e.printStackTrace();
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
		System.out.println(sql);
		return sql;
	}

	public static String buildConditionalDataQuery(String tableName,
			String[] attribs, int lo, int hi) {
		String baseQuery = buildDataQuery(tableName, attribs);
		String sql = baseQuery + " where id > " + lo + " AND id < " + hi;
		return sql;
	}

	public static Document[] loadTuplesToDocuments(String sqlQuery,
			String[] attribs) {
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList<Document> docList = new ArrayList<Document>();
		try (Connection databaseConnection = getDatabaseConnection()) {
			// retrieve the tuples to be indexed
			stmt = databaseConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
		              java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
			rs = stmt.executeQuery(sqlQuery);
			Runtime runtime = Runtime.getRuntime();
			System.out.println("Starting to retrieve data. Memory Used: "
					+ (runtime.totalMemory() - runtime.freeMemory()) / (1024*1024) + " MB");
			while (rs.next()) {
				Document doc = new Document();
				try {
					doc.add(new StoredField(FreebaseDataManager.FBID_ATTRIB, rs
							.getString(FreebaseDataManager.FBID_ATTRIB)));
					for (String attrib : attribs) {
						doc.add(new TextField(attrib, rs.getString(attrib),
								Field.Store.YES));
					}
				} catch (IllegalArgumentException e) { // the field is null
					// do nothing
				}
				docList.add(doc);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
		}
		return docList.toArray(new Document[0]);
	}

	// this method builds an index on lucene Documents
	public static String createIndex(Document[] docs, String[] attribs,
			String indexPath) {
		System.out.println("Creating Index...");
		cleanupIndexDir(indexPath);
		
		Directory directory = null;
		IndexWriter writer = null;
		try {
			// creating the index
			directory = FSDirectory.open(Paths.get(indexPath));
			StandardAnalyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			config.setOpenMode(OpenMode.CREATE);
			// .setRAMBufferSizeMB(256.0);
			writer = new IndexWriter(directory, config);
			for (Document doc : docs) {
				// System.out.println(rs.getString("name"));
				try {
					writer.addDocument(doc);
				} catch (IllegalArgumentException e) { // the field is null
					// do nothing
				}
			}
			System.out.println("Indexed documents: " + docs.length);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if (directory != null)
					directory.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return indexPath;
	}
	
	private static void cleanupIndexDir(String indexPath){
		File dirFile = new File(indexPath);
		if (dirFile.exists()) {
			for (File f : dirFile.listFiles()) {
				f.delete();
			}
		} else {
			dirFile.mkdir();
		}
	}

	public static void createShuffledTable(String tableName, String newTableName) {
		Statement stmt = null;
		try (Connection databaseConnection = getDatabaseConnection()) {
			stmt = databaseConnection.createStatement();
			stmt.executeUpdate("create table " + newTableName
					+ " as select * from table " + tableName
					+ " order by rand()");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

	public static void deleteTable(String tableName) {
		Statement stmt = null;
		try (Connection databaseConnection = getDatabaseConnection()) {
			stmt = databaseConnection.createStatement();
			stmt.executeUpdate("delete table " + tableName);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

	public static List<FreebaseQuery> getQueriesBySqlQuery(String sqlQuery) {
		List<FreebaseQuery> queries = new ArrayList<FreebaseQuery>();
		Statement st = null;
		ResultSet rs = null;
		try (Connection conn = getDatabaseConnection()) {
			st = conn.createStatement();
			rs = st.executeQuery(sqlQuery);
			while (rs.next()) {
				HashMap<String, String> attribs = new HashMap<String, String>();
				attribs.put(FreebaseDataManager.NAME_ATTRIB,
						rs.getString("text"));
				// attribs.put(DESC_ATTRIB, rs.getString("text"));
				FreebaseQuery q = new FreebaseQuery(rs.getInt("id"), attribs);
				q.text = rs.getString("text").trim().replace(",", " ");
				q.wiki = rs.getString("wiki_id");
				q.frequency = rs.getInt(FreebaseDataManager.FREQ_ATTRIB);
				q.fbid = rs.getString(FreebaseDataManager.FBID_ATTRIB);
				queries.add(q);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
			try {
				if (st != null)
					st.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
		}
		return queries;
	}

	public static List<FreebaseQuery> getQueriesByRelevancyTable(
			String tableName) {
		String sqlQuery = "select id, wiki_id, fbid, text, frequency from tbl_query as q where q.fbid in (select fbid from "
				+ tableName + ")";
		return getQueriesBySqlQuery(sqlQuery);
	}

	public static void removeKeyword(List<FreebaseQuery> queries, String pattern) {
		Pattern pat = Pattern.compile(pattern);
		for (FreebaseQuery query : queries) {
			// System.out.println(query.text);
			Matcher matcher = pat.matcher(query.text.toLowerCase());
			matcher.find();
			String keyword = matcher.group(0);
			query.attribs.put(FreebaseDataManager.NAME_ATTRIB, query.text
					.toLowerCase().replace(keyword, ""));
			// query.attribs.put(DESC_ATTRIB,
			// query.text.toLowerCase().replace(keyword, ""));
		}
	}

	public static void extractAndRemoveKeyword(List<FreebaseQuery> queries,
			String pattern) {
		Pattern pat = Pattern.compile(pattern);
		for (FreebaseQuery query : queries) {
			System.out.println(query.text);
			Matcher matcher = pat.matcher(query.text.toLowerCase());
			matcher.find();
			String keyword = matcher.group(0);
			query.attribs
					.put(FreebaseDataManager.SEMANTIC_TYPE_ATTRIB, keyword);
			query.attribs.put(FreebaseDataManager.NAME_ATTRIB, query.text
					.toLowerCase().replace(keyword, ""));
			// query.attribs.put(DESC_ATTRIB,
			// query.text.toLowerCase().replace(keyword, ""));
		}
	}

	public static void addIdenticalAttibuteToQueries(List<FreebaseQuery> list,
			String attrib, String val) {
		for (FreebaseQuery query : list) {
			query.attribs.put(attrib, val);
		}
	}

	@Deprecated
	public static void runQuery(FreebaseQuery freebaseQuery, String indexPath) {
		IndexReader reader = null;
		try {
			reader = DirectoryReader
					.open(FSDirectory.open(Paths.get(indexPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
			for (String attrib : freebaseQuery.attribs.keySet()) {
				builder.add(new QueryParser(attrib, new StandardAnalyzer())
						.parse(QueryParser.escape(freebaseQuery.attribs
								.get(attrib))), BooleanClause.Occur.SHOULD);
			}
			Query query = builder.build();
			// System.out.println("submitting query: " + query.toString());
			TopDocs topDocs = searcher.search(query,
					FreebaseDataManager.MAX_HITS);
			// System.out.println("hits length: " + hits.length);
			for (int i = 0; i < topDocs.scoreDocs.length; i++) {
				Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
				if (doc.get(FreebaseDataManager.FBID_ATTRIB).equals(
						freebaseQuery.fbid)) {
					// System.out.println(searcher.explain(query, hits[i].doc));
					freebaseQuery.relRank = i + 1;
					break;
				}
			}
			int precisionBoundry = topDocs.scoreDocs.length > 10 ? 10
					: topDocs.scoreDocs.length;
			for (int i = 0; i < precisionBoundry; i++) {
				Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
				if (i < 3)
					freebaseQuery.hits[i] = doc
							.get(FreebaseDataManager.NAME_ATTRIB);
			}

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// same code as above but returns an object of FreebaseQueryResult class
	public static FreebaseQueryResult runFreebaseQuery(
			FreebaseQuery freebaseQuery, String indexPath) {
		IndexReader reader = null;
		FreebaseQueryResult fqr = new FreebaseQueryResult();
		try {
			reader = DirectoryReader
					.open(FSDirectory.open(Paths.get(indexPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
			for (String attrib : freebaseQuery.attribs.keySet()) {
				builder.add(new QueryParser(attrib, new StandardAnalyzer())
						.parse(QueryParser.escape(freebaseQuery.attribs
								.get(attrib))), BooleanClause.Occur.SHOULD);
			}
			Query query = builder.build();
			// System.out.println("submitting query: " + query.toString());
			TopDocs topDocs = searcher.search(query,
					FreebaseDataManager.MAX_HITS);
			// System.out.println("hits length: " + hits.length);
			for (int i = 0; i < topDocs.scoreDocs.length; i++) {
				Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
				if (doc.get(FreebaseDataManager.FBID_ATTRIB).equals(
						freebaseQuery.fbid)) {
					// System.out.println(searcher.explain(query, hits[i].doc));
					freebaseQuery.relRank = i + 1;
					fqr.relRank = i + 1;
					break;
				}
			}
			int precisionBoundry = topDocs.scoreDocs.length > 3 ? 3
					: topDocs.scoreDocs.length;
			for (int i = 0; i < precisionBoundry; i++) {
				Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
				fqr.top3Hits[i] = doc.get(FreebaseDataManager.NAME_ATTRIB);
			}

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fqr;
	}

	// same as above but runs more than one query
	public static List<FreebaseQueryResult> runFreebaseQueries(
			List<FreebaseQuery> queries, String indexPath) {
		IndexReader reader = null;
		List<FreebaseQueryResult> fqrList = new ArrayList<FreebaseQueryResult>();
		try {
			reader = DirectoryReader
					.open(FSDirectory.open(Paths.get(indexPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			for (int i = 0; i < queries.size(); i++) {
				FreebaseQueryResult fqr = new FreebaseQueryResult();
				BooleanQuery.Builder builder = new BooleanQuery.Builder();
				for (String attrib : queries.get(i).attribs.keySet()) {
					builder.add(new QueryParser(attrib, new StandardAnalyzer())
							.parse(QueryParser.escape(queries.get(i).attribs
									.get(attrib))), BooleanClause.Occur.SHOULD);
				}
				Query query = builder.build();
				// System.out.println("submitting query: " + query.toString());
				TopDocs topDocs = searcher.search(query,
						FreebaseDataManager.MAX_HITS);
				// System.out.println("hits length: " + hits.length);
				for (int j = 0; j < topDocs.scoreDocs.length; j++) {
					Document doc = searcher.doc(topDocs.scoreDocs[j].doc);
					if (doc.get(FreebaseDataManager.FBID_ATTRIB).equals(
							queries.get(i).fbid)) {
						// System.out.println(searcher.explain(query,
						// hits[i].doc));
						queries.get(i).relRank = j + 1;
						fqr.relRank = j + 1;
						break;
					}
				}
				int precisionBoundry = topDocs.scoreDocs.length > 3 ? 3
						: topDocs.scoreDocs.length;
				for (int j = 0; j < precisionBoundry; j++) {
					Document doc = searcher.doc(topDocs.scoreDocs[j].doc);
					fqr.top3Hits[j] = doc.get(FreebaseDataManager.NAME_ATTRIB);
				}
				fqrList.add(fqr);
			}

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fqrList;
	}

	public static void createSampledTables(String tableName, int tableNumber) {
		Statement st = null;
		String newName = tableName;
		try (Connection conn = getDatabaseConnection()) {
			st = conn.createStatement();
			for (int i = 1; i <= tableNumber; i++) {
				String sql = "create table " + newName + "_" + i
						+ " as select * from " + tableName + " where mod("
						+ tableName + ".counter, " + tableNumber + ") < " + i
						+ ";";
				st.executeUpdate(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (st != null)
					st.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
		}
	}

}
