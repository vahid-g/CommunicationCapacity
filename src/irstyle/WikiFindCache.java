package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiFilesPaths;

public class WikiFindCache {

	public static int TOPDOC_COUNTS = 100;

	public static void main(String[] args) throws IOException, ParseException {

		WikiFilesPaths paths = WikiFilesPaths.getMaplePaths();
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
				paths.getMsnQrelFilePath());
		Collections.shuffle(queries, new Random(1));
		queries = queries.subList(0, 50);
		String tableName = "tbl_article_09";
		for (int i = 1; i <= 100; i += 1) {
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
						String docId = doc.get(WikiTableIndexer.QREL_FIELD);
						result.addResult(docId, "no title");
					}
					queryResults.add(result);
				}
			}
			double mrr = 0;
			for (QueryResult qr : queryResults) {
				mrr += qr.mrr();
			}
			mrr /= queryResults.size();
			System.out.println("index: " + i + " mrr = " + mrr);
		}

	}

}
