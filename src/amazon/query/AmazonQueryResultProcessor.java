package amazon.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import query.ExperimentQuery;
import query.Qrel;
import query.QueryResult;
import amazon.AmazonIsbnPopularityMap;

public class AmazonQueryResultProcessor {

	private static final Logger LOGGER = Logger
			.getLogger(AmazonQueryResultProcessor.class.getName());

	public static void convertIsbnAnswersToLtidAndFilter(QueryResult queryResult, Map<String, String> isbnToLtid) {
			List<String> newResults = new ArrayList<String>();
			List<String> newResultsTitle = new ArrayList<String>();
			for (int i = 0; i < queryResult.getTopResults().size(); i++) {
				String isbn = queryResult.getTopResults().get(i);
				String ltid = isbnToLtid.get(isbn);
				if (ltid == null) {
					LOGGER.log(Level.SEVERE, "Couldn't find ISBN: " + isbn
							+ " in dict");
					continue;
				}
				if (!newResults.contains(ltid)) {
					newResults.add(ltid);
					newResultsTitle.add(queryResult.getTopResults().get(i));
				}
			}
			queryResult.setTopResults(newResults);
			queryResult.setTopResultsTitle(newResultsTitle);
	}

	public static String generateLog(QueryResult queryResult,
			Map<String, Set<String>> ltidToIsbn, AmazonIsbnPopularityMap aipm) {
		ExperimentQuery query = queryResult.query;
		StringBuilder sb = new StringBuilder();
		sb.append("qid: " + query.getId() + "\t" + queryResult.mrr() + "\n");
		sb.append("query: " + query.getText() + "\n\n");
		sb.append("|relevant tuples| = " + query.getQrels().size() + "\n");
		sb.append("|returned results| = " + queryResult.getTopResults().size()
				+ "\n");
		int counter = 0;
		sb.append("returned results: \n");
		for (int i = 0; i < queryResult.getTopResults().size(); i++) {
			String returnedLtid = queryResult.getTopResults().get(i);
			String returnedTitle = queryResult.getTopResultsTitle().get(i);
			String isbn = returnedTitle
					.substring(0, returnedTitle.indexOf(':'));
			if (query.hasQrelId(returnedLtid)) {
				sb.append("++ " + returnedLtid + "\t" + returnedTitle + "\t"
						+ aipm.getWeight(isbn) + "\n");
			} else {
				sb.append("-- " + returnedLtid + "\t" + returnedTitle + "\t"
						+ aipm.getWeight(isbn) + "\n");
			}
			if (counter++ > 10)
				break;
		}
		counter = 0;
		sb.append("missed docs: ");
		for (Qrel qrel : query.getQrels()) {
			String relevantLtid = qrel.getQrelId();
			if (!queryResult.getTopResults().contains(relevantLtid)) {
				Set<String> isbns = ltidToIsbn.get(relevantLtid);
				if (isbns == null) {
					LOGGER.log(Level.SEVERE,
							"puuu, couldn't find isbns for ltid: "
									+ relevantLtid);
					continue;
				}
				for (String isbn : isbns) {
					sb.append(relevantLtid + ": (" + isbn + ", "
							+ aipm.getWeight(isbn) + ") ");
				}
				sb.append("\n");
			}
			if (counter++ > 10)
				break;
		}
		sb.append("-------------------------------------\n");
		return sb.toString();
	}

}
