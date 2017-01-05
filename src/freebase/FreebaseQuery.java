package freebase;

import java.util.HashMap;

public class FreebaseQuery {
	public FreebaseQuery(int id, HashMap<String, String> attribs) {
		this.id = id;
		this.attribs = attribs;
		this.relRank = -1;
	}

	int id;
	int frequency;
	String text;
	String wiki;
	HashMap<String, String> attribs;
	String fbid;
	int relRank;
	
	@Deprecated 
	String[] hits = new String[3];

	public double mrr() {
		if (relRank != -1) return 1.0 / relRank;
		else return 0;
	}

	public double p3() {
		if (relRank != -1 && relRank < 4)
			return 0.3;
		else
			return 0;
	}


	public double precisionAtK(int k){
		if (relRank != -1 && relRank <= k){
			return 1.0 / k;
		} else {
			return 0;
		}
	}
}