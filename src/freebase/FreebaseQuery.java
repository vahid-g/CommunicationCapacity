package freebase;

import java.util.HashMap;

public class FreebaseQuery {

    int id;
    int frequency;
    String text;
    String wiki;
    HashMap<String, String> attribs;
    String fbid;
    int instanceId;

    public FreebaseQuery(int id, HashMap<String, String> attribs) {
	this.id = id;
	this.attribs = attribs;
    }

    public FreebaseQuery(int instanceId, FreebaseQuery query) {
	this.id = query.id;
	this.frequency = query.frequency;
	this.text = query.text;
	this.wiki = query.wiki;
	this.fbid = query.fbid;
	this.instanceId = instanceId;
    }

}