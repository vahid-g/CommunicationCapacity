package query;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryResult {

    public class TopDocument {
	public final String id;
	public final String title;
	public final String explanation;

	public TopDocument(String id, String title, String explanation) {
	    this.id = id;
	    this.title = title;
	    this.explanation = explanation;
	}

	@Override
	public boolean equals(Object other) {
	    if (other == null)
		return false;
	    if (other == this)
		return true;
	    if (!(other instanceof TopDocument))
		return false;
	    TopDocument otherTopDocument = (TopDocument) other;
	    return (this.id.equals(otherTopDocument.id));
	}
    }

    private static final Logger LOGGER = Logger.getLogger(QueryResult.class
	    .getName());

    public ExperimentQuery query;
    private List<TopDocument> topDocuments = new ArrayList<TopDocument>();

    public QueryResult(ExperimentQuery query) {
	this.query = query;
    }

    public void addResult(String docId, String docTitle) {
	addResult(docId, docTitle, "");
    }

    public void addResult(String docId, String docTitle, String explanation) {
	TopDocument doc = new TopDocument(docId, docId + ": " + docTitle,
		explanation);
	topDocuments.add(doc);
    }

    public double averagePrecision() {
	double pk = precisionAtK(1);
	double ap = pk;
	for (int k = 1; k < topDocuments.size(); k++) {
	    if (query.hasQrelId(topDocuments.get(k).id))
		ap += (pk * k + 1) / (k + 1);
	    pk = (pk * k) / (k + 1);
	}
	return ap / query.getQrelScoreMap().size();
    }

    public double idcg(int p) {
	List<Integer> qrelScoreList = new ArrayList<Integer>();
	if (query.getQrelScoreMap().size() > 0)
	    qrelScoreList.addAll(query.getQrelScoreMap().values());
	Collections.sort(qrelScoreList, Collections.reverseOrder());
	double dcg = 0;
	for (int i = 0; i < Math.min(qrelScoreList.size(), p); i++) {
	    dcg += qrelScoreList.get(i) / (Math.log(i + 2) / Math.log(2));
	}
	return dcg;
    }

    public double mrr() {
	for (int i = 0; i < topDocuments.size(); i++) {
	    if (query.hasQrelId(topDocuments.get(i).id)) {
		return (1.0 / (i + 1));
	    }
	}
	return 0;
    }

    public double ndcg(int p) {
	double dcg = 0;
	for (int i = 0; i < Math.min(p, topDocuments.size()); i++) {
	    String currentResult = topDocuments.get(i).id;
	    if (query.getQrelScoreMap().containsKey(currentResult)) {
		dcg += query.getQrelScoreMap().get(currentResult)
			/ (Math.log(i + 2) / Math.log(2));
	    }
	}
	double idcg = idcg(p);
	if (idcg == 0)
	    return 0;
	return dcg / idcg;
    }

    public double precisionAtK(int k) {
	double truePositives = 0;
	for (int i = 0; i < Math.min(k, topDocuments.size()); i++) {
	    if (query.getQrelScoreMap().containsKey(topDocuments.get(i).id))
		truePositives++;
	}
	return truePositives / k;
    }

    public double recallAtK(int k) {
	double truePositives = 0;
	for (int i = 0; i < Math.min(k, topDocuments.size()); i++) {
	    if (query.getQrelScoreMap().containsKey(topDocuments.get(i).id))
		truePositives++;
	}
	return truePositives / query.getQrelScoreMap().size();
    }

    public String listFalseNegatives(int k) {
	StringBuilder sb = new StringBuilder();
	int counter = 0;
	for (String qrel : query.getQrelScoreMap().keySet()) {
	    if (!this.containsTopDocument(qrel)) {
		sb.append(query.getId() + "," + query.getText() + "," + qrel
			+ "\n");
	    }
	    if (++counter > k)
		break;
	}
	return sb.toString();
    }

    public String miniLog(Map<String, Double> idPopMap, int k) {
	StringBuilder sb = new StringBuilder();
	sb.append(this.query.getId() + ": " + this.query.getText()
		+ " |rets| = " + topDocuments.size() + "\n");
	int counter = 1;
	Set<String> rels = this.query.getQrelScoreMap().keySet();
	for (int i = 0; i < topDocuments.size(); i++) {
	    String docId = topDocuments.get(i).id;
	    if (counter++ <= k || rels.contains(docId)
		    || (i == Math.min(999, topDocuments.size() - 1))) {
		String rel1Char = "-";
		if (rels.contains(docId)) {
		    rel1Char = "+";
		}
		String explanation = topDocuments.get(i).explanation;
		sb.append(String.format("\t %s (%d) \t %s \t %s \t %s \n",
			rel1Char, i, idPopMap.get(docId),
			topDocuments.get(i).title, explanation));
	    }
	}
	sb.append("======================\n");
	return sb.toString();
    }

    public String resultString() {
	return query.getText() + "," + precisionAtK(10) + ","
		+ precisionAtK(20) + "," + mrr() + "," + averagePrecision()
		+ "," + recallAtK(200) + "," + recallAtK(1000) + "," + ndcg(10);
    }

    @Override
    public String toString() {
	return query.getId() + "," + query.getText();
    }

    public static void logResultsWithPopularity(List<QueryResult> results,
	    Map<String, Double> idPopMap, String logFilePath, int k) {
	LOGGER.log(Level.INFO, "Logging query results..");
	try (FileWriter fw = new FileWriter(logFilePath)) {
	    for (QueryResult result : results) {
		fw.write(result.miniLog(idPopMap, k));
	    }
	} catch (IOException e) {
	    LOGGER.log(Level.SEVERE, e.getMessage(), e);
	}
    }

    public List<TopDocument> getTopDocuments() {
	return topDocuments;
    }

    public void setTopDocuments(List<TopDocument> topDocuments) {
	this.topDocuments = topDocuments;
    }

    public boolean containsTopDocument(String id) {
	for (TopDocument topDocument : topDocuments) {
	    if (topDocument.id.equals(id)) {
		return true;
	    }
	}
	return false;
    }

}
