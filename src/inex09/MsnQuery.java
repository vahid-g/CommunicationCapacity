package inex09;

import java.util.List;

public class MsnQuery {
	
	
	public String text;
	public List<String> qrels;
	
	int qid;
	
	public MsnQuery(String text, List<String> qrels, int qid){
		this.text = text;
		this.qrels = qrels;
		this.qid = qid;
	}

}
