package query;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExperimentQuery {

	private static final Logger LOGGER = Logger.getLogger(ExperimentQuery.class.getName());

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

	public void addRelevantAnswer(Qrel qrel) {
		if (qrel.getQid() != this.id){
			LOGGER.log(Level.SEVERE, "Query and Qrel ids don't match!!!");
		}
		qrels.add(qrel);
	}

	@Override
	public boolean equals(Object obj) {
		ExperimentQuery query = (ExperimentQuery) obj;
		return this.id.equals(query.id);
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

	public boolean hasQrelId(String qrelId) {
		for (Qrel qrel : this.qrels) {
			if (qrel.getQrelId().equals(qrelId))
				return true;
		}
		return false;
	}

	public void setText(String text) {
		this.text = text;
	};

	@Override
	public String toString() {
		return "query: " + this.text;
	}

}