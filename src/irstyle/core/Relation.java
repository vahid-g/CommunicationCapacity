package irstyle.core;

import java.util.*;
import java.lang.Boolean;

//import com.ms.wfc.ui.*;

public class Relation {
	String name;
	private Vector attributes;
	private Vector inMasterIndex; // Boolean Vector. true if corresponding attribute
									// is considered in Master Index creation
	private Vector attrType;// types of the corresponding attributes
	private Vector refRelations;// Vector of adjacent relations
	private Vector refRelAttr;
	int size;// #tuples in Relation
	// attribute name of this Relation that references corresponding relation of
	// refRelations

	public Relation(String nam) {
		name = nam;
		attributes = new Vector(1);
		refRelations = new Vector(1);
		refRelAttr = new Vector(1);
		inMasterIndex = new Vector(1);
		attrType = new Vector(1);
	}

	public String getName() {
		return name;
	}

	public void addAttribute(String name, boolean inMI, String type) {
		attributes.addElement(name);
		inMasterIndex.addElement(new Boolean(inMI));
		attrType.addElement(type);
	}

	Vector getAttributes() {
		return attributes;
	}

	String getAttribute(int i) {
		return (String) attributes.elementAt(i);
	}

	int getNumAttributes() {
		return attributes.size();
	}

	public void addAttr4Rel(String attr, String relname) {
		refRelAttr.addElement(attr);
		refRelations.addElement(relname);
	}

	String getAttr4Rel(String relname) {// returns the attribute name that references relation relname
										// if more than one attributes, getAttrVector4Rel should be used
		for (int i = 0; i < refRelations.size(); i++)
			if (((String) refRelations.elementAt(i)).compareTo(relname) == 0)
				return (String) refRelAttr.elementAt(i);
		return null;
	}

	Vector getAttrVector4Rel(String relname) {// returns the attribute names String Vector that references relation
												// relname
		Vector v = new Vector(1);
		for (int i = 0; i < refRelations.size(); i++)
			if (((String) refRelations.elementAt(i)).compareTo(relname) == 0)
				v.addElement((String) refRelAttr.elementAt(i));
		return v;
	}

	public void setSize(int s) {
		size = s;
	}

	int getSize() {
		return size;
	}

	boolean isInMasterIndex(String attrName) {
		for (int i = 0; i < attributes.size(); i++)
			if (((String) attributes.elementAt(i)).compareTo(attrName) == 0)
				return ((Boolean) inMasterIndex.elementAt(i)).booleanValue();
		System.out.println("attr not found ,isInMasterIndex");
		return false;

	}

	String getAttrType(String attrName) {
		for (int i = 0; i < attributes.size(); i++)
			if (((String) attributes.elementAt(i)).compareTo(attrName) == 0)
				return (String) attrType.elementAt(i);
		System.out.println("attr not found, getAttrType");
		return null;

	}

	double getSelectivity(Relation rel) {
		if (rel == null)
			return 1;
		return ((double) rel.getSize()) / ((double) getSize());
	}

}
