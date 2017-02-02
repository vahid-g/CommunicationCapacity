package inex09;

import java.util.ArrayList;

public class InexQuery {

	String text;
	private ArrayList<String> relDocs;
	double mrr;
	double p3;
	double p10;

	public InexQuery(String text, String relDoc) {
		this.text = text;
		this.relDocs = new ArrayList<String>();
		this.relDocs.add(relDoc);
	}

	public String getFirstRelDoc() {
		return relDocs.get(0);
	}

	public void addRelevantAnswer(String relDoc) {
		relDocs.add(relDoc);
	}

	@Override
	public String toString() {
		return "query: " + this.text + " relDoc: " + this.relDocs.get(0);
	}

	public ArrayList<String> getRelDocs() {
		return relDocs;
	}

}