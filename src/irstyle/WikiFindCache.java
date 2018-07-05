package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import database.DatabaseType;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiFilesPaths;

public class WikiFindCache {

	public static int TOPDOC_COUNTS = 100;

	public static void main(String[] args) throws IOException, SQLException, ParseException {
		if (args[0].equals("articles")) {
			findArticleCache();
		} else if (args[0].equals("images")) {
			findOtherCache("tbl_image_pop", "tbl_article_image_09", "image_id");
		} else if (args[0].equals("links")) {
			findOtherCache("tbl_link_pop", "tbl_article_link_09", "link_id");
		} else {
			System.out.println("Wrong input args!");
		}
	}

	public static void findArticleCache() throws IOException, ParseException {
		String tableName = "tbl_article_09";
		WikiFilesPaths paths = WikiFilesPaths.getMaplePaths();
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
				paths.getMsnQrelFilePath());
		Collections.shuffle(queries, new Random(1));
		queries = queries.subList(0, 50);
		for (int i = 1; i <= 100; i += 1) {
			List<QueryResult> queryResults = runQueriesOnIndex(tableName, queries, i);
			double mrr = 0;
			for (QueryResult qr : queryResults) {
				mrr += qr.mrr();
			}
			mrr /= queryResults.size();
			System.out.println("index: " + i + " mrr = " + mrr);
		}

	}

	public static void findOtherCache(String tableName, String joinTableName, String idAttrib)
			throws IOException, SQLException, ParseException {
		WikiFilesPaths paths = WikiFilesPaths.getMaplePaths();
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
				paths.getMsnQrelFilePath());
		Collections.shuffle(queries, new Random(1));
		queries = queries.subList(0, 50);
		updateQrelsForOtherTables(queries, joinTableName, idAttrib);
		for (int i = 1; i <= 100; i += 1) {
			List<QueryResult> queryResults = runQueriesOnIndex(tableName, queries, i);
			double mrr = 0;
			for (QueryResult qr : queryResults) {
				mrr += qr.mrr();
			}
			mrr /= queryResults.size();
			System.out.println("index: " + i + " mrr = " + mrr);
		}
	}

	private static void updateQrelsForOtherTables(List<ExperimentQuery> queries, String tableName, String idAttrib)
			throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			Connection conn = dc.getConnection();
			for (ExperimentQuery query : queries) {
				Map<String, Integer> qrelScoreMap = new HashMap<String, Integer>();
				for (String articleId : query.getQrelScoreMap().keySet()) {
					String sql = "select " + idAttrib + " from " + tableName + " where article_id = " + articleId;
					try (Statement stmt = conn.createStatement()) {
						ResultSet rs = stmt.executeQuery(sql);
						while (rs.next()) {
							qrelScoreMap.put(rs.getString(idAttrib), 1);
						}
					}
				}
				query.setQrelScoreMap(qrelScoreMap);
			}
		}
	}

	private static List<QueryResult> runQueriesOnIndex(String tableName, List<ExperimentQuery> queries, int i)
			throws ParseException, IOException {
		String indexPath = "/data/ghadakcv/wikipedia/" + tableName + "/" + i;
		List<QueryResult> queryResults = new ArrayList<QueryResult>();
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			QueryParser qp = new QueryParser(WikiTableIndexer.TEXT_FIELD, new StandardAnalyzer());
			for (ExperimentQuery q : queries) {
				QueryResult result = new QueryResult(q);
				Query query = qp.parse(q.getText());
				ScoreDoc[] scoreDocHits = searcher.search(query, TOPDOC_COUNTS).scoreDocs;
				for (int j = 0; j < Math.min(TOPDOC_COUNTS, scoreDocHits.length); j++) {
					Document doc = reader.document(scoreDocHits[j].doc);
					String docId = doc.get(WikiTableIndexer.ID_FIELD);
					result.addResult(docId, "no title");
				}
				queryResults.add(result);
			}
		}
		return queryResults;
	}
}
