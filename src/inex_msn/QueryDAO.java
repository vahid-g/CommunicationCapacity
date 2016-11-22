package inex_msn;

import java.util.ArrayList;

class QueryDAO {
	
		
	public QueryDAO(String text, String relDoc) {
		this.text = text;
		this.relDocs = new ArrayList<String>();
		this.relDocs.add(relDoc);
	}
	
	public String getFirstRelDoc(){
		return relDocs.get(0);
	}
	
	public void addRelevantAnswer(String relDoc){
		relDocs.add(relDoc);
	}

	String text;
	private ArrayList<String> relDocs;
	double mrr;
	double p3;
	double p10;
	
	@Override
	public String toString() {
		return "query: " + this.text + " relDoc: " + this.relDocs.get(0);
	}

	public ArrayList<String> getRelDocs() {
		return relDocs;
	}
	
}