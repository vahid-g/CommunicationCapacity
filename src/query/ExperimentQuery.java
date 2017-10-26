package query;

import java.util.HashSet;
import java.util.Set;

public class ExperimentQuery {

	private Integer id;
	private String text;
	private Set<Qrel> qrels;

	public ExperimentQuery(int id, String text) {
		this.id = id;
		this.text = text;
		qrels = new HashSet<Qrel>();
	}
	
	public ExperimentQuery(int id, String text, Set<Qrel> qrels) {
		this.id = id;
		this.text = text;
		this.qrels = qrels;
	}

	public Integer getId() {
		return id;
	}

	public Set<Qrel> getQrels() {
		return qrels;
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
	
	public boolean hasReturnedQrelid(String qrelId){
		for (Qrel qrel : this.qrels) {
			if (qrel.getQrelId().equals(qrelId))
				return true;
		}
		return false;
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