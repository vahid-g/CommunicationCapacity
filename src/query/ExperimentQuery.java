package query;

import java.util.ArrayList;
import java.util.List;

public class ExperimentQuery {

	int id;
	public String text;
	public List<String> qrels;
	
	public ExperimentQuery(int id, String text){
		this.id = id;
		this.text = text;
		qrels = new ArrayList<String>();
	}
	
	public ExperimentQuery(int id, String text, List<String> qrels){
		this.id = id;
		this.text = text;
		this.qrels = qrels;
	}
	
	public ExperimentQuery(String text, String relDoc) {
		this.text = text;
		this.qrels.add(relDoc);
	}
	
	public void setQrels(List<String> qrels) {
		this.qrels = qrels;
	}

	public String getFirstRelDoc() {
		return qrels.get(0);
	}

	public void addRelevantAnswer(String relDoc) {
		qrels.add(relDoc);
	}

	@Override
	public String toString() {
		return "query: " + this.text;
	}

	public List<String> getRelDocs() {
		return qrels;
	}

}