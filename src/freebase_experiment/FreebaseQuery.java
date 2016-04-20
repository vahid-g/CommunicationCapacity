package freebase_experiment;

import java.util.HashMap;

public class FreebaseQuery{
	public FreebaseQuery(int id, HashMap<String, String> attribs) {
		this.id = id;
		this.attribs = attribs;
		this.mrr = 0;
		this.p3 = 0;
		this.p10 = 0;
	}

	int id;
	int frequency;
	String text;
	String wiki;
	HashMap<String, String> attribs;
	String fbid;

	double mrr;
	double p3;
	double p10;
	String[] hits = new String[3];
}