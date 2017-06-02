package query;

import java.util.HashSet;
import java.util.Set;

public class ExperimentQuery {

	Integer id;
	private String text;
	public Set<String> qrels;
	public ExperimentQuery(int id, String text) {
		this.id = id;
		this.text = text;
		qrels = new HashSet<String>();
	}

	public ExperimentQuery(int id, String text, Set<String> qrels) {
		this.id = id;
		this.text = text;
		this.qrels = qrels;
	}

	public String getText() {
		return text.replace(",", " ");
	}

	public void setText(String text) {
		this.text = text;
	}

	public void addRelevantAnswer(String relDoc) {
		qrels.add(relDoc);
	}

	public Set<String> getRelDocs() {
		return qrels;
	}

	@Override
	public boolean equals(Object obj) {
		ExperimentQuery query = (ExperimentQuery) obj;
		return this.id.equals(query.id);
	};

	@Override
	public String toString() {
		return "query: " + this.text;
	}

}