package amazon.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.index.IndexReader;

import amazon.popularity.AmazonIsbnPopularityMap;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;

public class AmazonQueryResultProcessor {

	private static final Logger LOGGER = Logger.getLogger(AmazonQueryResultProcessor.class.getName());

	public static void convertIsbnAnswersToLtidAndFilter(QueryResult queryResult, Map<String, String> isbnToLtid) {
		List<QueryResult.TopDocument> newTopDocuments = new ArrayList<QueryResult.TopDocument>();
		for (QueryResult.TopDocument doc : queryResult.getTopDocuments()) {
			String isbn = doc.id;
			String ltid = isbnToLtid.get(isbn);
			if (ltid == null) {
				LOGGER.log(Level.SEVERE, "Couldn't find ISBN: " + isbn + " in dict");
				continue;
			}
			QueryResult.TopDocument topDocument = queryResult.new TopDocument(ltid, doc.title, doc.explanation);
			if (!newTopDocuments.contains(topDocument)) {
				newTopDocuments.add(topDocument);
			}
		}
		queryResult.setTopDocuments(newTopDocuments);
	}

	public static String generateLog(QueryResult queryResult, Map<String, Set<String>> ltidToIsbn,
			AmazonIsbnPopularityMap aipm, IndexReader reader) {
		ExperimentQuery query = queryResult.query;
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + query.getId() + "\t p@10: " + queryResult.precisionAtK(10) + "\n");
		sb.append("query: " + query.getText() + "\n");
		sb.append("|relevant tuples| = " + query.getQrelScoreMap().size() + "\n");
		sb.append("|returned results| = " + queryResult.getTopDocuments().size() + "\n");
		int counter = 0;
		sb.append("top returned results: \n");
		for (int i = 0; i < queryResult.getTopDocuments().size(); i++) {
			String returnedLtid = queryResult.getTopDocuments().get(i).id;
			String returnedTitle = queryResult.getTopDocuments().get(i).title;
			String isbn = returnedTitle.substring(0, returnedTitle.indexOf(':'));
			if (query.hasQrelId(returnedLtid)) {
				sb.append("++ " + returnedLtid + "\t" + returnedTitle + "\t" + aipm.getWeight(isbn) + "\n");
			} else {
				sb.append("-- " + returnedLtid + "\t" + returnedTitle + "\t" + aipm.getWeight(isbn) + "\n");
			}
			if (counter++ > 10)
				break;
		}
		sb.append("=== missed docs === \n");
		for (String relevantLtid : query.getQrelScoreMap().keySet()) {
			if (!queryResult.containsTopDocument(relevantLtid)) {
				Set<String> isbns = ltidToIsbn.get(relevantLtid);
				if (isbns == null) {
					LOGGER.log(Level.SEVERE, "puuu, couldn't find isbns for ltid: " + relevantLtid);
					continue;
				}
				sb.append("ltid: " + relevantLtid + "\n");
				for (String isbn : isbns) {
					int inIndex = QueryServices.lookupDocumentId(isbn, reader);
					if (inIndex > 0)
						sb.append("\t ++(" + isbn + ", " + aipm.getWeight(isbn) + ") \n");
					else
						sb.append("\t --(" + isbn + ", " + aipm.getWeight(isbn) + ") \n");
				}
			}
		}
		sb.append("-------------------------------------\n");
		return sb.toString();
	}

}
