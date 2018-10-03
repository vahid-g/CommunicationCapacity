package query;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExperimentQuery {

	private static final Logger LOGGER = Logger.getLogger(ExperimentQuery.class.getName());

	private Integer id;
	private String text;
	private Map<String, Integer> qrelScoreMap = new HashMap<String, Integer>();
	private int freq;

	public ExperimentQuery(ExperimentQuery query) {
		this.id = query.id;
		this.text = query.text;
		this.qrelScoreMap = new HashMap<String, Integer>(query.qrelScoreMap);
		this.freq = query.freq;
	}

	public ExperimentQuery(int id, String text, int freq) {
		this.id = id;
		this.text = text;
		this.freq = freq;
	}

	public ExperimentQuery(int id, String text, Set<Qrel> qrels) {
		this(id, text, 1, qrels);
	}

	public int getFreq() {
		return freq;
	}

	public ExperimentQuery(int id, String text, int freq, Set<Qrel> qrels) {
		this(id, text, freq);
		for (Qrel qrel : qrels) {
			qrelScoreMap.put(qrel.getQrelId(), qrel.getRel());
		}
	}

	public void addRelevantAnswer(Qrel qrel) {
		if (qrel.getQid() != this.id) {
			LOGGER.log(Level.SEVERE, "Query and Qrel ids don't match!!!");
		} else if (qrelScoreMap.keySet().contains(qrel.getQrelId())) {
			LOGGER.log(Level.SEVERE, "Qrel already has been added to the query!");
		} else {
			qrelScoreMap.put(qrel.getQrelId(), qrel.getRel());
		}
	}

	@Override
	public boolean equals(Object obj) {
		ExperimentQuery query = (ExperimentQuery) obj;
		return this.id.equals(query.id);
	}

	public Integer getId() {
		return id;
	}

	public Map<String, Integer> getQrelScoreMap() {
		return qrelScoreMap;
	}

	public String getText() {
		return text.replace(",", " ");
	}

	public boolean hasQrelId(String qrelId) {
		return qrelScoreMap.containsKey(qrelId);
	}

	public void setQrelScoreMap(Map<String, Integer> qrelIdQrelScore) {
		this.qrelScoreMap = qrelIdQrelScore;
	}

	public void setText(String text) {
		this.text = text;
	};

	@Override
	public String toString() {
		return "query: " + this.text;
	}

}