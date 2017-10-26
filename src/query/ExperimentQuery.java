package query;

import java.util.HashSet;
import java.util.Set;

public class ExperimentQuery {

	Integer id;
	private String text;
	public Set<Qrel> qrels;

	public ExperimentQuery(int id, String text) {
		this.id = id;
		this.text = text;
		qrels = new HashSet<Qrel>();
	}

	public Integer getId() {
		return id;
	}

	public Set<Qrel> getQrels() {
		return qrels;
	}

	public ExperimentQuery(int id, String text, Set<Qrel> qrels) {
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

	public void addRelevantAnswer(Qrel qrel) {
		qrels.add(qrel);
	}

	public Set<Qrel> getRelDocs() {
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