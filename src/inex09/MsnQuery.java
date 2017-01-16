package inex09;

import java.util.List;

public class MsnQuery {
	
	String text;
	
	List<String> qrels;
	
	public MsnQuery(String text, List<String> qrels){
		this.text = text;
		this.qrels = qrels;
	}

}
