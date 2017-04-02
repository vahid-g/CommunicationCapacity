package inex09;

import java.util.ArrayList;
import java.util.List;

public class InexQuery {

	int id;
	public String text;
	public List<String> relDocs = new ArrayList<String>();
	public double mrr;
	public double p3;
	public double p10;
	
	
	public InexQuery(int id, String text){
		this.id = id;
		this.text = text;
	}
	
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
		return "query: " + this.text;
	}

	public List<String> getRelDocs() {
		return relDocs;
	}

}