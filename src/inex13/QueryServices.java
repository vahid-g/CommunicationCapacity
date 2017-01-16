package inex13;

import inex09.InexQueryServices;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

public class QueryServices {

	public static List<InexQueryResult> runQueries(List<InexQueryDAO> queries,
			String indexPath) {
		List<InexQueryResult> results = new ArrayList<InexQueryResult>();
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexPath)))) {
			System.out.println("Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			for (InexQueryDAO queryDAO : queries) {
				Query query = InexQueryServices.buildLuceneQuery(queryDAO.text,
						Experiment.TITLE_ATTRIB,
						Experiment.CONTENT_ATTRIB);
				int threshold = 20;
				TopDocs topDocs = searcher.search(query, threshold);
				InexQueryResult result = new InexQueryResult();
				result.query = queryDAO;
				for (int i = 0; i < topDocs.scoreDocs.length; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					int docId = Integer.parseInt(doc
							.get(Experiment.DOCNAME_ATTRIB));
					result.returnedDocs.add(docId);
				}
				results.add(result);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return results;
	}
}
