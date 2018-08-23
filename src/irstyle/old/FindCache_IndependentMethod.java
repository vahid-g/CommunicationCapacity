package irstyle.old;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
import irstyle.WikiIndexer;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiFilesPaths;

public class FindCache_IndependentMethod {

	private static int TOPDOC_COUNTS = 100;

	public static void main(String[] args) throws IOException, SQLException, ParseException {
		List<String> argsList = Arrays.asList(args);
		WikiFilesPaths paths = WikiFilesPaths.getMaplePaths();

		List<ExperimentQuery> queries = null;
		if (argsList.contains("msn")) {
			queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(), paths.getMsnQrelFilePath());
		} else if (argsList.contains("inex")) {
			queries = QueryServices.loadInexQueries(paths.getInexQueryFilePath(), paths.getInexQrelFilePath());
		}
		Collections.shuffle(queries, new Random(1));
		queries = queries.subList(0, 20);
		if (argsList.contains("articles")) {
			findCache(queries, "tbl_article_wiki13");
		} else if (argsList.contains("images")) {
			updateQrelsForOtherTables(queries, "tbl_article_image_09", "image_id");
			findCache(queries, "tbl_image_pop");
		} else if (argsList.contains("links")) {
			updateQrelsForOtherTables(queries, "tbl_article_link_09", "link_id");
			findCache(queries, "tbl_link_pop");
		} else {
			System.out.println("Wrong input args!");
		}
	}

	public static void findCache(List<ExperimentQuery> queries, String tableName) throws IOException, ParseException {
		double prevAcc = 0;
		double acc = 0;
		for (int i = 1; i <= 100; i += 1) {
			prevAcc = acc;
			List<QueryResult> queryResults = runQueriesOnLuceneIndex(tableName, queries, i);
			acc = 0;
			for (QueryResult qr : queryResults) {
//				acc += qr.precisionAtK(20);
//				acc += qr.recallAtK(100);
				acc += qr.mrr();
			}
			acc /= queryResults.size();
			System.out.printf("index size = %d%% eff = %.2f\n", i, acc);
			if (prevAcc > acc) {
				break;
			}
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

	private static List<QueryResult> runQueriesOnLuceneIndex(String tableName, List<ExperimentQuery> queries, int i)
			throws ParseException, IOException {
		String indexPath = "/data/ghadakcv/wikipedia/" + tableName + "/" + i;
		List<QueryResult> queryResults = new ArrayList<QueryResult>();
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(reader);
			QueryParser qp = new QueryParser(WikiIndexer.TEXT_FIELD, new StandardAnalyzer());
			for (ExperimentQuery q : queries) {
				QueryResult result = new QueryResult(q);
				Query query = qp.parse(QueryParser.escape(q.getText()));
				ScoreDoc[] scoreDocHits = searcher.search(query, TOPDOC_COUNTS).scoreDocs;
				for (int j = 0; j < Math.min(TOPDOC_COUNTS, scoreDocHits.length); j++) {
					Document doc = reader.document(scoreDocHits[j].doc);
					String docId = doc.get(WikiIndexer.ID_FIELD);
					result.addResult(docId, "no title");
				}
				queryResults.add(result);
			}
		}
		return queryResults;
	}
}
