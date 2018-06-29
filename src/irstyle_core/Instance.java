package irstyle_core;
//package xkeyword;

import java.math.*;
import java.util.*;

import irstyle.SimpleMain;

//import com.ms.wfc.ui.*;

//Instance is the Tuple Set of the paper
public class Instance implements Cloneable// , iCandidateNetwork
{
	protected MIndexAccess MIndx;

	boolean hasKeyword = false;
	Vector adjList; // Instance Vector
	Vector incoming; // String Vector. "inc" if there is an incoming list from the instance in
						// adj_list to this instance, else "out"
	int sizeAdjList = 0;
	String relationName; // relation of the instance
	public Vector keywords; // list of keywords in this instance
	String objID = "no"; // Id of the target object. "no" means "not set".
	int numKeywords = 0;
	int currInst = 0; // used to traverse the adjList
	boolean isIntermResult = false;// set to true if this is an interm.result (name INT_RES*)
	// if adjList[i] is a fragment joiningAttr[i] is the zero based index of the
	// attr on which
	// this is joined to adjList[i]
	// eg: if adjList[i]=YPP and joiningAttr[i]=2, then it joins on the second P.
	Vector joiningAttr;
	Vector fragmDirections;// used if this is a fragment. eg: C->Y->P->P has "out","out","out"
	Vector attributes;

	Instance() {
		adjList = new Vector(1);
		incoming = new Vector(1);
		keywords = new Vector(1);
		fragmDirections = new Vector(1);
		joiningAttr = new Vector(1);
		attributes = new Vector(1);
	}

	void addAttribute(String attr) {
		attributes.addElement(attr);
	}

	String getAttribute(int i) {
		return (String) attributes.elementAt(i);
	}

	void addAdjInstance(Instance inst, String inc) {
		adjList.addElement(inst);
		sizeAdjList++;
		incoming.addElement(inc);
		joiningAttr.addElement(new Integer(0));
	}

	void addKeyword(String keyw) {
		keywords.addElement(keyw);
		numKeywords++;
		hasKeyword = true;
	}

	boolean haskeyword(String k) {
		for (int i = 0; i < numKeywords; i++)
			if (((String) keywords.elementAt(i)).compareTo(k) == 0)
				return true;
		return false;
	}

	void removeAllKeywords() {
		keywords.removeAllElements();
		hasKeyword = false;
		numKeywords = 0;
	}

	Instance getFirstAdjInstance() {
		if (adjList.size() < 1)
			return null;
		currInst = 0;
		return (Instance) adjList.elementAt(currInst);
	}

	Instance getNextAdjInstance() // should be avoided, bug prone
	{
		if (currInst > sizeAdjList - 2)
			return null;
		return (Instance) adjList.elementAt(++currInst);
	}

	int getSizeAdjList() {
		return sizeAdjList;
	}

	public String getRelationName() {
		return relationName;
	}

	void setRelationName(String str) {
		relationName = str;
	}

	boolean isIntResult() {
		return isIntermResult;
	}

	void setIntResult(boolean b) {
		isIntermResult = b;
	}

	private Instance getInstance2(String relName, Vector visitedNodes) {// used by getInstance, to avoid visiting the
																		// same node more than once and going into loops
		if (getRelationName().compareTo(relName) == 0)
			return this;
		Instance ins = getFirstAdjInstance();
		if (ins != null)
			if (!stringContained(visitedNodes, ins.getRelationName())) {
				visitedNodes.addElement(ins.getRelationName());
				Instance in2 = ins.getInstance2(relName, visitedNodes);
				if (in2 != null)
					return in2;
			}
		for (int i = 1; i < getSizeAdjList(); i++) {
			ins = getNextAdjInstance();
			if (ins != null)
				if (!stringContained(visitedNodes, ins.getRelationName())) {
					visitedNodes.addElement(ins.getRelationName());
					Instance in2 = ins.getInstance2(relName, visitedNodes);
					if (in2 != null)
						return in2;
				}
		}
		return null;
	}

	Instance getInstance(String relName) // return an instance with specified relation name(unique if we call it from a
											// Schema object)
	{
		Vector visitedNodes = new Vector(1);
		return getInstance2(relName, visitedNodes);
	}

	protected boolean stringContained(Vector strV, String str) {// checks if str is in String Vector strV
		for (int i = 0; i < strV.size(); i++)
			if (str.compareTo((String) strV.elementAt(i)) == 0)
				return true;
		return false;
	}

	public int getsize() {// size is #joins=#nodes-1
		return getAllInstances().size() - 1;
	}

	private void getAllInstances2(Instance in, Vector visitedNodes) {
		visitedNodes.addElement(in);
		for (int i = 0; i < in.adjList.size(); i++) {
			Instance in2 = (Instance) in.adjList.elementAt(i);
			if (!visitedNodes.contains(in2))
				getAllInstances2(in2, visitedNodes);
		}
	}

	public Vector getAllInstances() {// returns a Vector of all Tuple sets in this JNTS
		Vector visitedNodes = new Vector(1);
		getAllInstances2(this, visitedNodes);
		return visitedNodes;
	}

	private Instance clone2(Instance inst) {
		Instance in = new Instance();
		in.hasKeyword = inst.hasKeyword;
		in.adjList = (Vector) inst.adjList.clone(); // Instance Vector
		in.incoming = (Vector) inst.incoming.clone();
		in.fragmDirections = (Vector) inst.fragmDirections.clone();
		in.joiningAttr = (Vector) inst.joiningAttr.clone();
		in.attributes = (Vector) inst.attributes.clone();
		in.sizeAdjList = inst.sizeAdjList;
		in.relationName = inst.relationName; // relation of the instance
		in.keywords = (Vector) inst.keywords.clone(); // list of keywords in this instance
		in.numKeywords = inst.numKeywords;
		in.currInst = inst.currInst;
		in.isIntermResult = inst.isIntermResult;
		;
		return in;
	}

	public synchronized Object clone() {
		// clone each Instance in adjList
		Vector newInstances = new Vector(1);
		Vector allInstances = getAllInstances();
		for (int i = 0; i < allInstances.size(); i++) {
			Instance temp = clone2((Instance) allInstances.elementAt(i));
			newInstances.addElement(temp);
		}
		for (int i = 0; i < allInstances.size(); i++)
			for (int j = 0; j < ((Instance) newInstances.elementAt(i)).adjList.size(); j++) {
				Instance temp = (Instance) newInstances.elementAt(i);
				temp.adjList.setElementAt(newInstances.elementAt(allInstances.indexOf(temp.adjList.elementAt(j))), j);
			}
		return (Instance) newInstances.elementAt(allInstances.indexOf(this));
	}

	Instance getTupleSet() {// returns a copy of this Instance with an empty adjList
		try {
			Instance in = new Instance();
			in = (Instance) this.clone();
			in.adjList.removeAllElements();
			in.incoming.removeAllElements();
			in.sizeAdjList = 0;
			return in;
		} catch (Exception e1) {
			// MessageBox.show("exception class: " + e1.getClass() +" with message: " +
			// e1.getMessage(),"exception");
			return null;
		}
	}

	Vector getKeywCombinations(Vector keywV) {// returns a Vector of String Vectors, one for each combination of strings
												// in keywV
		try {
			Vector ret = new Vector(1);
			int numCombs = (int) Math.round(Math.pow(2, keywV.size()));
			for (int i = 0; i < numCombs; i++) {
				Vector oneComb = new Vector(1);
				for (int keywIndex = 0; keywIndex < keywV.size(); keywIndex++)
					if (i / (Math.round(Math.pow(2, keywIndex))) % 2 == 1)
						oneComb.addElement(keywV.elementAt(keywIndex));
				ret.addElement(oneComb);
			}
			return ret;

		} catch (Exception e1) {
			// MessageBox.show("exception class: " + e1.getClass() +" with message: " +
			// e1.getMessage(),"exception in getKeywCombinations");
			return null;
		}
	}
	/*
	 * boolean duplicateInAdjList(Instance in) {//checks if there are two Instances
	 * in adjList of in with same relationname Vector temp; for(int
	 * i=0;i<adjList.size();i++)
	 * 
	 * 
	 * }
	 */

	private void getExpansions2(Vector visitedNodes, Vector expansions, Instance in, Schema sch) {
		try {
			// schInstance is the node of the schema graph of the tuple set's (ins) relation
			Instance schInstance = sch.getInstance(in.getRelationName());
			// if(schInstance==null) MessageBox.show("tuple set not found in
			// schema","error");
			Instance expIns = schInstance.getFirstAdjInstance();
			Instance TSexpIns; // expIns with empty adjList
			if (expIns != null) {
				String direction = (String) schInstance.incoming.elementAt(schInstance.currInst);
				TSexpIns = expIns.getTupleSet();
				// add TSexpIns to newJNTS for all combinations of keywords
				// replaced on 6/19/02 to use the info on which tuple sets are not empty
				// Vector keywCombinations= getKeywCombinations(expIns.keywords);
				Vector keywCombinations = MIndx.getKeywComb4Relname(expIns.relationName);
				for (int i = 0; i < keywCombinations.size(); i++) {
					Instance TSexpInsNew = (Instance) TSexpIns.clone();
					TSexpInsNew.keywords = (Vector) keywCombinations.elementAt(i);
					Instance newJNTS = (Instance) in.clone();

					newJNTS.addAdjInstance(TSexpInsNew, direction);
					if (direction.compareTo("out") == 0)
						TSexpInsNew.addAdjInstance(newJNTS, "inc");
					else
						TSexpInsNew.addAdjInstance(newJNTS, "out");
					expansions.addElement(newJNTS);
				}
			}
			for (int j = 1; j < schInstance.getSizeAdjList(); j++) {
				expIns = schInstance.getNextAdjInstance();
				if (expIns != null) {
					String direction = (String) schInstance.incoming.elementAt(schInstance.currInst);
					TSexpIns = expIns.getTupleSet();
					// add TSexpIns to newJNTS for all combinations of keywords
					// replaced on 6/19/02 to use the info on which tuple sets are not empty
					// Vector keywCombinations= getKeywCombinations(expIns.keywords);
					Vector keywCombinations = MIndx.getKeywComb4Relname(expIns.relationName);
					for (int i = 0; i < keywCombinations.size(); i++) {
						Instance TSexpInsNew = (Instance) TSexpIns.clone();
						TSexpInsNew.keywords = (Vector) keywCombinations.elementAt(i);
						Instance newJNTS = (Instance) in.clone();
						if (direction.compareTo("out") == 0)
							TSexpInsNew.addAdjInstance(newJNTS, "inc");
						else
							TSexpInsNew.addAdjInstance(newJNTS, "out");
						newJNTS.addAdjInstance(TSexpInsNew, direction);
						expansions.addElement(newJNTS);
					}
				}
			}

			Instance ins = in.getFirstAdjInstance();
			if (ins != null)
				if (!visitedNodes.contains(ins)) {
					visitedNodes.addElement(ins);
					getExpansions2(visitedNodes, expansions, ins, sch);

				}
			for (int j = 1; j < in.getSizeAdjList(); j++) {
				ins = in.getNextAdjInstance();
				if (ins != null)
					if (!visitedNodes.contains(ins)) {
						visitedNodes.addElement(ins);
						getExpansions2(visitedNodes, expansions, ins, sch);
					}
			}
		} catch (Exception e1) {
			// MessageBox.show("exception class: " + e1.getClass() +" with message: " +
			// e1.getMessage(),"exception in getExpansions2");
		}
	}

	protected Vector getExpansions(Instance JNTS, Schema sch) {// returns an Instance Vector of all expansions of a JNTS
																// by adding one tuple set
		Vector visitedNodes = new Vector(1); // Vector of Instance
		visitedNodes.addElement(JNTS);
		Vector expansions = new Vector(1); // Vector of expansion Instances
		getExpansions2(visitedNodes, expansions, JNTS, sch);
		return expansions;
	}

	void getKeywVectors(Vector visitedNodes, Vector vv, Instance JNTS) {
		visitedNodes.addElement(JNTS);
		if (!JNTS.keywords.isEmpty())
			vv.addElement(JNTS.keywords);
		for (int i = 0; i < JNTS.adjList.size(); i++) {
			Instance in = (Instance) JNTS.adjList.elementAt(i);
			if (!visitedNodes.contains(in))
				getKeywVectors(visitedNodes, vv, in);
		}
	}

	Vector mergeVectors(Vector vv) {
		Vector v = new Vector(1);
		for (int i = 0; i < vv.size(); i++)
			for (int j = 0; j < ((Vector) vv.elementAt(i)).size(); j++) {
				if (!stringContained(v, ((String) ((Vector) vv.elementAt(i)).elementAt(j))))
					v.addElement(((String) ((Vector) vv.elementAt(i)).elementAt(j)));
			}
		return v;
	}

	protected boolean RedundantTS(Instance JNTS) {// returns true in JNTS is redundant
													// check if we can replace a TS with a free TS and still have all
													// keywords
													// create a Vector of keyword Vectors, one for each tuple set, then
													// create a Vector
													// of all keywords , then remove a keyword Vector at a time and see
													// if all keywords still there
		Vector vv = new Vector(1); // Vector of Vectors
		Vector visitedNodes = new Vector(1); // Vector of Instances
		getKeywVectors(visitedNodes, vv, JNTS);
		Vector allKeyw = mergeVectors(vv);
		int numKeyw = allKeyw.size();
		for (int excluded = 0; excluded < vv.size(); excluded++) {
			Vector vvExcl = (Vector) vv.clone();
			vvExcl.removeElementAt(excluded);
			Vector vExcl = mergeVectors(vvExcl);
			if (vExcl.size() == numKeyw)
				return true;
		}
		return false;
	}

	boolean many2many(Instance in1, Instance in2, Schema sch) {// 3/28/02 returns true if it is in1->in2 and in1<-in2 in
																// schema graph

		boolean foundr = false; // true if in1->in2
		boolean foundl = false; // true if in1<-in2
		Instance in1sch = sch.getInstance(in1.getRelationName());
		Instance in2sch = sch.getInstance(in2.getRelationName());
		for (int i = 0; i < in1sch.adjList.size(); i++) {
			Instance intemp = (Instance) in1sch.adjList.elementAt(i);
			if (intemp.equals(in2sch)) {
				if (((String) in1sch.incoming.elementAt(i)).compareTo("out") == 0)
					foundr = true;
				else
					foundl = true;
			}
		}
		return (foundr && foundl);
	}

	protected boolean twoIdentTuples(Schema sch) {// I only check the case A-> B <-A', where -> is primary to foreign,
													// A, A' are two tuple sets of the same relation A
													// 3/28/02: In the case of many to many relationships, where A->B
													// and A<-B, it returns false
													// 10/28/02 if P1->R<-P2 return true except if R=PP
		Vector allInst = getAllInstances();
		for (int i = 0; i < allInst.size(); i++) {
			Instance in = (Instance) allInst.elementAt(i);
			for (int j = 0; j < in.adjList.size(); j++) {
				if (!many2many(in, (Instance) in.adjList.elementAt(j), sch)) // 3/28/02
					for (int k = 0; k < in.adjList.size(); k++)
						if ((k != j
								&& ((Instance) in.adjList.elementAt(j)).getRelationName()
										.compareTo(((Instance) in.adjList.elementAt(k)).getRelationName()) == 0
								&& ((String) in.incoming.elementAt(j)).compareTo("inc") == 0
								&& ((String) in.incoming.elementAt(k)).compareTo("inc") == 0) || // 10/28/02
								(k != j && ((Instance) in.adjList.elementAt(j)).getRelationName().compareTo("P1") == 0
										&& ((Instance) in.adjList.elementAt(k)).getRelationName().compareTo("P2") == 0
										&& in.getRelationName().compareTo("PP") != 0
										&& ((String) in.incoming.elementAt(j)).compareTo("inc") == 0
										&& ((String) in.incoming.elementAt(k)).compareTo("inc") == 0)
								|| // 10/28/02
								(k != j && ((Instance) in.adjList.elementAt(j)).getRelationName().compareTo("P2") == 0
										&& ((Instance) in.adjList.elementAt(k)).getRelationName().compareTo("P1") == 0
										&& in.getRelationName().compareTo("PP") != 0
										&& ((String) in.incoming.elementAt(j)).compareTo("inc") == 0
										&& ((String) in.incoming.elementAt(k)).compareTo("inc") == 0))
							return true;

			}
		}
		return false;
	}

	protected boolean containsAllKeywords(Vector keywset) {// checks if the JNTS contains all keywords in keywset
															// we assume no other keywords in this
		Vector vv = new Vector(1); // Vector of Vectors
		Vector visitedNodes = new Vector(1); // Vector of Instances
		getKeywVectors(visitedNodes, vv, this);
		Vector allKeyw = mergeVectors(vv);// all keywords in this
		if (allKeyw.size() != keywset.size())
			return false;
		return true;
	}

	protected boolean hasFreeTSLeaf() {// returns true if this has a free tuple set as leaf
		Vector allInst = getAllInstances();
		for (int i = 0; i < allInst.size(); i++) {
			Instance in = (Instance) allInst.elementAt(i);
			if ((in.adjList.size() == 1) && (in.keywords.size() == 0))
				return true;
		}
		return false;
	}

	private String getTupleSet4Instance(Instance inst1, Vector allkeywords) {// returns the tuple set name for the root
																				// Instance of inst1
																				// format for tuple sets is
																				// relationName_k1_.._.._kn, where
																				// k1..kn are the
																				// indices of the keywords in inst1 in
																				// allkeywords
		String name = inst1.getRelationName();
		if (!inst1.keywords.isEmpty()) // 10/6/02
			name = SimpleMain.TUPLESET_PREFIX + "_" + name;
		/*
		 * if(inst1.isIntResult()) return name; for(int i=0;i<allkeywords.size();i++)
		 * if(stringContained(inst1.keywords, (String) allkeywords.elementAt(i)))
		 * name+="_"+i;
		 */
		return name;

	}

	String getSQLstatement(Vector relations, Vector allkeywords) {// inputs a Vector of all Relation objects and outputs
																	// the SQL statement that evaluates
																	// this candidate network
		String SQL = "select * from ";
		Vector v = this.getAllInstances();
		for (int i = 0; i < v.size(); i++) {
			// SQL+= ((Instance) v.elementAt(i)).getRelationName()+" r"+i+" ,";
			SQL += getTupleSet4Instance((Instance) v.elementAt(i), allkeywords) + " r" + i + " ,";
		}
		SQL = SQL.substring(0, SQL.length() - 1);
		if (v.size() == 0 || v.size() == 1)
			return SQL;
		SQL += " WHERE ";
		for (int i = 0; i < v.size(); i++) {
			Instance inst = (Instance) v.elementAt(i);
			Relation rel = getRelation(relations, inst.getRelationName());
			for (int j = 0; j < inst.adjList.size(); j++) {// we only take outgoing edges to avoid duplicates
				if (((String) inst.incoming.elementAt(j)).compareTo("out") == 0) {
					Instance inst2 = (Instance) inst.adjList.elementAt(j);
					Relation rel2 = getRelation(relations, inst2.getRelationName());
					// find index for inst2
					int inst2index = -1;
					for (int k = 0; k < v.size(); k++)
						if (inst2.equals((Instance) v.elementAt(k)))
							inst2index = k;
					// if(inst2index==-1) MessageBox.show("inst2index not found","error");
					SQL += " r" + i + "." + rel.getAttr4Rel(inst2.getRelationName()) + "=" + "r" + inst2index + "."
							+ rel2.getAttr4Rel(inst.getRelationName()) + " AND ";
				}
			}
		}
		SQL = SQL.substring(0, SQL.length() - 5); // remove last " AND "
		return SQL;
	}

	String getSQLstatementOrdered(Vector relations, Vector allkeywords) {// inputs a Vector of all Relation objects and
																			// outputs the SQL statement that evaluates
																			// this candidate network, ordered by sum
																			// scores/(CNsize+1)
		String SQL = "select * from ";
		String orderby = " order by (";
		Vector v = this.getAllInstances();
		for (int i = 0; i < v.size(); i++) {
			// SQL+= ((Instance) v.elementAt(i)).getRelationName()+" r"+i+" ,";
			String TS = getTupleSet4Instance((Instance) v.elementAt(i), allkeywords);
			SQL += TS + " r" + i + " ,";
			if (TS.charAt(0) == 'T')
				orderby += "r" + i + ".score +";
		}
		SQL = SQL.substring(0, SQL.length() - 1);
		if (v.size() == 0 || v.size() == 1)
			return SQL;
		SQL += " WHERE ";
		for (int i = 0; i < v.size(); i++) {
			Instance inst = (Instance) v.elementAt(i);
			Relation rel = getRelation(relations, inst.getRelationName());
			for (int j = 0; j < inst.adjList.size(); j++) {// we only take outgoing edges to avoid duplicates
				if (((String) inst.incoming.elementAt(j)).compareTo("out") == 0) {
					Instance inst2 = (Instance) inst.adjList.elementAt(j);
					Relation rel2 = getRelation(relations, inst2.getRelationName());
					// find index for inst2
					int inst2index = -1;
					for (int k = 0; k < v.size(); k++)
						if (inst2.equals((Instance) v.elementAt(k)))
							inst2index = k;
					// if(inst2index==-1) MessageBox.show("inst2index not found","error");
					SQL += " r" + i + "." + rel.getAttr4Rel(inst2.getRelationName()) + "=" + "r" + inst2index + "."
							+ rel2.getAttr4Rel(inst.getRelationName()) + " AND ";
				}
			}
		}
		SQL = SQL.substring(0, SQL.length() - 5); // remove last " AND "
		orderby = orderby.substring(0, orderby.length() - 1);
		SQL += orderby + ")/" + (this.getsize() + 1);
		return SQL;
	}

	public String getSQLstatementOrderedwScores(Vector relations, Vector allkeywords) {// inputs a Vector of all
																						// Relation
		// objects and outputs the SQL statement
		// that evaluates
		// this candidate network, ordered by
		// sum scores/(CNsize+1)
		// like getSQLstatementOrdered, but also
		// returns an attribute totalscore,
		// which has the score of the tuple
		String SQL = "select ";
		String select = "";
		String from = "";
		String orderby = " (";
		Vector v = this.getAllInstances();
		for (int i = 0; i < v.size(); i++) {
			// SQL+= ((Instance) v.elementAt(i)).getRelationName()+" r"+i+" ,";
			String TS = getTupleSet4Instance((Instance) v.elementAt(i), allkeywords);
			from += TS + " r" + i + " ,";
			Instance inst = (Instance) v.elementAt(i);
			Relation rel = getRelation(relations, inst.getRelationName());

			for (int j = 0; j < rel.getNumAttributes(); j++)
				select += "r" + i + "." + rel.getAttribute(j) + ",";

			if (TS.charAt(0) == 'T')
				orderby += "r" + i + ".score +";
		}
		select = select.substring(0, select.length() - 1);
		from = from.substring(0, from.length() - 1);
		orderby = orderby.substring(0, orderby.length() - 1) + ")/" + (this.getsize() + 1);
		SQL = "select " + select + "," + orderby + " totalscore from " + from;
		if (v.size() == 0 || v.size() == 1)
			return SQL + " order by totalscore desc";
		SQL += " WHERE ";
		for (int i = 0; i < v.size(); i++) {
			Instance inst = (Instance) v.elementAt(i);
			Relation rel = getRelation(relations, inst.getRelationName());
			for (int j = 0; j < inst.adjList.size(); j++) {// we only take outgoing edges to avoid duplicates
				if (((String) inst.incoming.elementAt(j)).compareTo("out") == 0) {
					Instance inst2 = (Instance) inst.adjList.elementAt(j);
					Relation rel2 = getRelation(relations, inst2.getRelationName());
					// find index for inst2
					int inst2index = -1;
					for (int k = 0; k < v.size(); k++)
						if (inst2.equals((Instance) v.elementAt(k)))
							inst2index = k;
					// if(inst2index==-1) MessageBox.show("inst2index not found","error");
					SQL += " r" + i + "." + rel.getAttr4Rel(inst2.getRelationName()) + "=" + "r" + inst2index + "."
							+ rel2.getAttr4Rel(inst.getRelationName()) + " AND ";
				}
			}
		}
		SQL = SQL.substring(0, SQL.length() - 5); // remove last " AND "
		SQL += " order by totalscore desc";
		return SQL;
	}

	public String getSQLstatementParameterized(Vector relations, Vector allkeywords,
			ArrayList nfreeTSs /* is output */) {// inputs
		// a Vector of all Relation objects and outputs the parametrized SQL statement that evaluates this candidate network
		// and also the names of non free TSs from which the parameters are instantiated
		String SQL = "select * from ";
		Vector v = this.getAllInstances();
		String parameters = "";
		for (int i = 0; i < v.size(); i++) {
			// SQL+= ((Instance) v.elementAt(i)).getRelationName()+" r"+i+" ,";
			Instance inst = (Instance) v.elementAt(i);
			String tupleset = getTupleSet4Instance(inst, allkeywords);
			SQL += tupleset + " r" + i + " ,";
			if (!inst.keywords.isEmpty()) {
				parameters += " AND " + "r" + i + ".id = ? ";
				nfreeTSs.add(tupleset);
			}
		}
		SQL = SQL.substring(0, SQL.length() - 1);
		if (v.size() == 0)
			return null;

		if (v.size() == 1)
			return (SQL + " where " + parameters.substring(4, parameters.length()));
		SQL += " WHERE ";
		for (int i = 0; i < v.size(); i++) {
			Instance inst = (Instance) v.elementAt(i);
			Relation rel = getRelation(relations, inst.getRelationName());
			for (int j = 0; j < inst.adjList.size(); j++) {// we only take outgoing edges to avoid duplicates
				if (((String) inst.incoming.elementAt(j)).compareTo("out") == 0) {
					Instance inst2 = (Instance) inst.adjList.elementAt(j);
					Relation rel2 = getRelation(relations, inst2.getRelationName());
					// find index for inst2
					int inst2index = -1;
					for (int k = 0; k < v.size(); k++)
						if (inst2.equals((Instance) v.elementAt(k)))
							inst2index = k;
					// if(inst2index==-1) MessageBox.show("inst2index not found","error");
					SQL += " r" + i + "." + rel.getAttr4Rel(inst2.getRelationName()) + "=" + "r" + inst2index + "."
							+ rel2.getAttr4Rel(inst.getRelationName()) + " AND ";
				}
			}
		}
		SQL = SQL.substring(0, SQL.length() - 5); // remove last " AND "
		SQL += " " + parameters;
		return SQL;
	}

	Relation getRelation(Vector relations, String name) {// inputs a Vector of Relations and outputs the one with name
															// name
		for (int i = 0; i < relations.size(); i++)
			if (((Relation) relations.elementAt(i)).getName().compareTo(name) == 0)
				return (Relation) relations.elementAt(i);
		return null;
	}

	boolean isTSIdentical(Instance in1, Instance in2) {// compares 2 tuples sets (ignores adjList)
														// first compare relation name
		if (in1.getRelationName().compareTo(in2.getRelationName()) != 0 /* 10/6/02 */
				&& !(in1.getRelationName().compareTo("P1") == 0 && in2.getRelationName().compareTo("P2") == 0)
				&& !(in2.getRelationName().compareTo("P1") == 0 && in1.getRelationName().compareTo("P2") == 0))
			return false;
		// now compare keywords
		if (in1.keywords.size() != in2.keywords.size())
			return false;
		for (int i = 0; i < in1.keywords.size(); i++)
			if (!stringContained(in2.keywords, ((String) in1.keywords.elementAt(i))))
				return false;
		return true;
	}

	boolean identical(Instance in2) {// checks if this Instance is identical to in2. A-B is not considered identical
										// to B-A
										// checking all cases would be very hard and slow, because it is like graph
										// isomorphism which is NP-complete
										// the code is not 100% correct.That is, there could be a case where this and
										// in2 are not identical,
										// yet identical returns true
		Vector v1 = getAllInstances();
		Vector v2 = in2.getAllInstances();
		if (v1.size() != v2.size())
			return false;
		for (int i = 0; i < v1.size(); i++) {
			Instance inst1 = (Instance) v1.elementAt(i);
			Instance inst2 = (Instance) v2.elementAt(i);
			if (!isTSIdentical(inst1, inst2))
				return false;
			if (inst1.adjList.size() != inst2.adjList.size())
				return false;
			for (int j = 0; j < inst1.adjList.size(); j++)
				if (!isTSIdentical((Instance) inst1.adjList.elementAt(j), (Instance) inst2.adjList.elementAt(j))
						|| ((String) inst1.incoming.elementAt(j)).compareTo((String) inst2.incoming.elementAt(j)) != 0)
					return false;

		}
		return true;
	}

	Vector getCreateTableStatement(Vector relations, Vector allkeywords, String tablename) {// inputs a Vector of all
																							// Relation objects and
																							// outputs 2 SQL statements
																							// that:
																							// a. creates table
																							// tablename and b. the
																							// statement that insert to
																							// the table
																							// the results of evaluating
																							// this candidate network
		Vector SQLstatements = new Vector(2);
		String sql = "CREATE TABLE  " + tablename + " (";
		Vector v = this.getAllInstances();
		for (int i = 0; i < v.size(); i++) {
			String relname = ((Instance) v.elementAt(i)).getRelationName();
			Relation rel = null;
			for (int j = 0; j < relations.size(); j++)
				if (((Relation) relations.elementAt(j)).getName().compareTo(relname) == 0)
					rel = (Relation) relations.elementAt(j);
			for (int j = 0; j < rel.getNumAttributes(); j++) {
				if (!(i == 0 && j == 0))
					sql += ", ";
				sql += rel.getAttribute(j) + "_" + i + " " + rel.getAttrType(rel.getAttribute(j));
			}
		}
		sql += ")";
		SQLstatements.addElement(sql);
		sql = "INSERT INTO " + tablename + " SELECT * FROM ";
		for (int i = 0; i < v.size(); i++) {
			sql += getTupleSet4Instance((Instance) v.elementAt(i), allkeywords) + " r" + i + " ,";
		}
		sql = sql.substring(0, sql.length() - 1);
		if (v.size() == 0 || v.size() == 1) {
			SQLstatements.addElement(sql);
			return SQLstatements;
		}
		sql += " WHERE ";
		for (int i = 0; i < v.size(); i++) {
			Instance inst = (Instance) v.elementAt(i);
			Relation rel = getRelation(relations, inst.getRelationName());
			for (int j = 0; j < inst.adjList.size(); j++) {// we only take outgoing edges to avoid duplicates
				if (((String) inst.incoming.elementAt(j)).compareTo("out") == 0) {
					Instance inst2 = (Instance) inst.adjList.elementAt(j);
					Relation rel2 = getRelation(relations, inst2.getRelationName());
					// find index for inst2
					int inst2index = -1;
					for (int k = 0; k < v.size(); k++)
						if (inst2.equals((Instance) v.elementAt(k)))
							inst2index = k;
					// if(inst2index==-1) MessageBox.show("inst2index not found","error");
					sql += " r" + i + "." + rel.getAttr4Rel(inst2.getRelationName()) + "=" + "r" + inst2index + "."
							+ rel2.getAttr4Rel(inst.getRelationName()) + " AND ";
				}
			}
		}
		sql = sql.substring(0, sql.length() - 5); // remove last " AND "

		SQLstatements.addElement(sql);
		return SQLstatements;
	}

	boolean allkeywInSchema(Vector allkeyw) {
		Vector v = this.getAllInstances();
		for (int i = 0; i < allkeyw.size(); i++) {
			String keyw = (String) allkeyw.elementAt(i);
			boolean foundit = false;
			for (int j = 0; j < v.size(); j++)
				if (stringContained(((Instance) v.elementAt(j)).keywords, keyw))
					foundit = true;
			if (!foundit)
				return false;
		}
		return true;
	}

	String getAttrRemote(int index) {// the joining attr name of the index-th adjacent remote relation
		if (index >= adjList.size())
			return null;
		Instance inremote = (Instance) adjList.elementAt(index);
		return inremote.getAttribute(((Integer) joiningAttr.elementAt(index)).intValue());
	}

	/**
	 * The joining attr index of the index-th adjacent remote relation
	 */
	int getAttrIdxRemote(int index) {
		return ((Integer) joiningAttr.elementAt(index)).intValue();
	}

	String getAttrLocal(int index) {// the joining attr name of this relation joining to the index-th adjacent
									// remote relation
		if (index >= adjList.size())
			return null;
		Instance inremote = (Instance) adjList.elementAt(index);
		int index_of_this_in_inremote = -1;
		for (int i = 0; i < inremote.adjList.size(); i++)
			if (inremote.adjList.elementAt(i).equals(this)) {
				index_of_this_in_inremote = i;
				break;
			}
		return getAttribute(((Integer) inremote.joiningAttr.elementAt(index_of_this_in_inremote)).intValue());
	}

	int getAttrIdxLocal(int index) {// the joining attr name of this relation joining to the index-th adjacent
									// remote relation
		Instance inremote = (Instance) adjList.elementAt(index);
		int index_of_this_in_inremote = -1;
		for (int i = 0; i < inremote.adjList.size(); i++)
			if (inremote.adjList.elementAt(i).equals(this)) {
				index_of_this_in_inremote = i;
				break;
			}
		return ((Integer) inremote.joiningAttr.elementAt(index_of_this_in_inremote)).intValue();
	}

	@Override
	public String toString() {
		return this.relationName;
	}
}
