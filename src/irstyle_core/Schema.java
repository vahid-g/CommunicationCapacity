package irstyle_core;
//package xkeyword;

import java.util.StringTokenizer;
import java.util.Vector;

import irstyle.IRStyleKeywordSearch;

/**
 * Each relation has exactly one instance for schema.
 */
public class Schema extends Instance {

	private Instance getInstancefromArray(Instance instArr[], String name) {
		if (getRelationName().compareTo(name) == 0)
			return (Instance) this;
		for (int i = 0; i < instArr.length; i++)
			if (instArr[i].getRelationName().compareTo(name) == 0)
				return instArr[i];
		return null;
	}

	Exception ShemaFormatException = new Exception("format error in schema_str");

	/**
	 * format of schema_str: num_relations name_1 name_2 name_n (*name_1 is the name
	 * of the this instance*) name_i name_j .... (*edge from name_i to name_j*)
	 */
	public Schema(String schema_str) {
		try {
			StringTokenizer strtok = new StringTokenizer(schema_str);
			int numRelations = 0;
			if (strtok.hasMoreTokens())
				numRelations = Integer.valueOf(strtok.nextToken()).intValue();
			else
				throw ShemaFormatException;
			Instance instArr[] = new Instance[numRelations - 1];
			// we end at numRelations-1 because the first instance is not in the array
			for (int i = 0; i < numRelations - 1; i++)
				instArr[i] = new Instance();
			if (strtok.hasMoreTokens())
				setRelationName(strtok.nextToken());
			else
				throw ShemaFormatException;
			for (int i = 0; i < numRelations - 1; i++) // add relations
			{
				if (strtok.hasMoreTokens())
					instArr[i].setRelationName(strtok.nextToken());
				else
					throw ShemaFormatException;
			}
			while (strtok.hasMoreTokens()) // add edges
			{
				Instance inst1 = getInstancefromArray(instArr, strtok.nextToken());
				Instance inst2;
				if (strtok.hasMoreTokens())
					inst2 = getInstancefromArray(instArr, strtok.nextToken());
				else
					throw ShemaFormatException;// Applet1.printInStatusBar("format error in schema_str");
				if (inst1 == null || inst2 == null)
					throw ShemaFormatException;
				inst1.addAdjInstance(inst2, "out");
				inst2.addAdjInstance(inst1, "inc");
			}

			// this=instArr[0];

		} catch (Exception e1) {
			// MessageBox.show("exception class: " + e1.getClass() +" with message: " +
			// e1.getMessage(),"exception");
		}

	}

	private class Keyword {
		String keyw;
		int frequency;

		Keyword(String k) {
			keyw = k;
			frequency = 1;
		}
	}

	private void addInstanceKeyword(Vector keywV, String k) {// increases the frequency of the keyword in the keyword
																// vector if it exists, otherwise
																// it adds the keyword to the Vector
		boolean found = false;
		for (int i = 0; i < keywV.size(); i++)
			if (((Keyword) keywV.elementAt(i)).keyw.compareTo(k) == 0) {
				((Keyword) keywV.elementAt(i)).frequency++;
				found = true;
			}
		if (!found) {
			Keyword k1 = new Keyword(k);
			keywV.addElement(k1);
		}
	}

	private void fillKeywV(Vector keywV, Instance in, Vector visitedNodes) {// adds to the Keyword Vector keywV, all
																			// keywords and their frequencies from all
																			// instances connected to in whose name is
																			// not in visitedNodes
		for (int i = 0; i < in.keywords.size(); i++)
			addInstanceKeyword(keywV, (String) in.keywords.elementAt(i));
		for (int i = 0; i < in.adjList.size(); i++) {
			Instance ins = (Instance) in.adjList.elementAt(i);
			if (ins != null && !stringContained(visitedNodes, ins.getRelationName())) {
				visitedNodes.addElement((String) ins.getRelationName());
				fillKeywV(keywV, ins, visitedNodes);
			}
		}
	}

	private String findLeastFreqKeyw() {
		Vector keywV = new Vector(1); // vector of Keyword objects
		Vector visitedNodes = new Vector(1); // names of relations already visited
		visitedNodes.addElement((String) getRelationName());
		fillKeywV(keywV, (Instance) this, visitedNodes);
		int minfreq = 100000;
		String lfKeyw = null;
		for (int i = 0; i < keywV.size(); i++)
			if (((Keyword) keywV.elementAt(i)).frequency < minfreq) {
				lfKeyw = ((Keyword) keywV.elementAt(i)).keyw;
				minfreq = ((Keyword) keywV.elementAt(i)).frequency;
			}
		return lfKeyw;
	}

	private void getInstancesWithKeyw2(String keyw, Vector visitedNodes, Vector instances, Instance in) {// used by
																											// getInstancesWithKeyw,
																											// to avoid
																											// visiting
																											// the same
																											// node more
																											// than once
																											// and going
																											// into
																											// loops
																											// and also
																											// to add
																											// all
																											// instances
																											// to
																											// instances
																											// before
																											// returning
																											// them
																											// (that's
																											// why it
																											// does not
																											// return
																											// instances)
		if (in.haskeyword(keyw))
			instances.addElement(in);
		for (int i = 0; i < in.adjList.size(); i++) {
			Instance ins = (Instance) in.adjList.elementAt(i);
			if (ins != null)
				if (!stringContained(visitedNodes, ins.getRelationName())) {
					visitedNodes.addElement(ins.getRelationName());
					getInstancesWithKeyw2(keyw, visitedNodes, instances, ins);
				}
		}
	}

	private Vector getInstancesWithKeyw(String keyw) {// returns the Vector of Instances that contain the keyword keyw
		Vector visitedNodes = new Vector(1);
		visitedNodes.addElement(this.getRelationName());
		Vector instances = new Vector(1);
		getInstancesWithKeyw2(keyw, visitedNodes, instances, this);
		// create fresh objects for each Instance in instances and delete adjlist for
		// them
		Vector freshInstances = new Vector(1);
		for (int i = 0; i < instances.size(); i++) {
			Instance in = ((Instance) instances.elementAt(i)).getTupleSet();
			freshInstances.addElement(in);
		}
		return freshInstances;
	}

	private Vector getInstancesWithKeyw() {// returns the Vector of Instances that contain any keyword
		Vector allinst = this.getAllInstances();
		Vector freshInstances = new Vector(1);
		for (int i = 0; i < allinst.size(); i++) {
			Instance inst = (Instance) allinst.elementAt(i);
			if (!inst.keywords.isEmpty()) {
				Instance in = ((Instance) allinst.elementAt(i)).getTupleSet();
				freshInstances.addElement(in);
			}
		}
		return freshInstances;
	}

	Vector allKeywComb4Instances(Vector Q, String lfkeyw) {// for each Instance I in Q, add all clones with a subset S
															// of keywords of I, where lfkeyw in S
		Vector ret = new Vector(1);
		for (int i = 0; i < Q.size(); i++) {
			// replaced on 6/19/02 to use the info on which tuple sets are not empty
			// Vector keywComb=getKeywCombinations(((Instance) Q.elementAt(i)).keywords );
			Vector keywComb = MIndx.getKeywComb4Relname(((Instance) Q.elementAt(i)).relationName);
			for (int j = 0; j < keywComb.size(); j++)
				if (stringContained((Vector) keywComb.elementAt(j), lfkeyw)) {
					Instance temp = (Instance) ((Instance) Q.elementAt(i)).clone();
					temp.keywords = (Vector) keywComb.elementAt(j);
					ret.addElement(temp);
				}
		}
		return ret;
	}

	void addVector(Vector v1, Vector v2) {// appends v2 to the end of v1
		for (int i = 0; i < v2.size(); i++)
			v1.addElement(v2.elementAt(i));
	}

	boolean pruningCondition(Instance JNTS, Schema sch) {// checks i) if we can replace a TS with a free TS and still
															// have all keywords
															// ii) 2 tuples identical
															// returns true if JNTS must be pruned out

		// first check if we can replace a TS with a free TS and still have all keywords
		// if (RedundantTS(JNTS) ) return true; //9/30/02
		if (JNTS.twoIdentTuples(sch))
			return true;
		return false;
	}

	boolean acceptCondition(Instance JNTS, Vector allkeyw) {
		// if(!JNTS.containsAllKeywords(allkeyw)) return false; //9/30/02
		if (JNTS.hasFreeTSLeaf())
			return false;
		return true;
	}

	Vector pruneExpansions(Vector expansions, int maxsize, Vector allkeyw, Schema sch) {// 6/19/02 Prune expansions with
																						// no chance to contain all
																						// keywords in future.
																						// if #keyw=n, prune all
																						// expansions with a node with
																						// adjlist of size >n
																						// if #keyw=2, prune all
																						// expansions with a node with
																						// adjlist of size >2

		for (int i = 0; i < expansions.size(); i++) {
			Instance in = (Instance) expansions.elementAt(i);
			Vector v = in.getAllInstances();
			for (int j = 0; j < v.size(); j++) {
				if (((Instance) v.elementAt(j)).adjList.size() > allkeyw.size()) {
					expansions.removeElementAt(i);
					i--;
					break;
				}
			}
		}
		return expansions;

	}

	int numFreeTSs(Instance JNTS) {
		int count = 0;
		Vector allinst = JNTS.getAllInstances();
		for (int i = 0; i < allinst.size(); i++)
			if (!((Instance) allinst.elementAt(i)).keywords.isEmpty())
				count++;
		return count;
	}

	public Vector getCNs(int maxsize, Vector allkeyw, Schema sch, MIndexAccess MI) {
		MIndx = MI;
		Vector CNs = new Vector(1);
		// check if all keywords are present in schema
		if (!allkeywInSchema(allkeyw))
			return CNs;

		// find keyword in minimum # of relations
		// String lfkeyw=findLeastFreqKeyw();//10/16/02
		// add to Q all Instances(fresh copies with empty adjList) that contain lfkeyw
		// Vector Q=getInstancesWithKeyw(lfkeyw); //queue of algorithm //10/16/02
		Vector Q = getInstancesWithKeyw(); // queue of algorithm //10/16/02, ANY KEYWORD
		// Q= allKeywComb4Instances(Q, lfkeyw);//adds all possible instances that
		// contain lfkeyw //9/30/02
		while (!Q.isEmpty()) {
			// 6/8/2002. produce at most 50 CNs
			if (CNs.size() > IRStyleKeywordSearch.MAX_GENERATED_CNS)
				break;
			// end 6/8/2002
			Instance JNTS = (Instance) Q.firstElement(); // I is current active instance(joining net. of tuple sets)
			Q.removeElementAt(0);
			if (pruningCondition(JNTS, sch)) // R->S<-R construct. //rejected
			{
			} else if (numFreeTSs(JNTS) > allkeyw.size()) // rejected
			{
			} else if (JNTS.getsize() > maxsize) // rejected
			{
			} else if (JNTS.getsize() == maxsize) // check if accepted but will not be expanded
			{
				if (acceptCondition(JNTS, allkeyw))
					CNs.addElement(JNTS);
			} else if (numFreeTSs(JNTS) == allkeyw.size()) // check if accepted but will not be expanded
			{
				if (acceptCondition(JNTS, allkeyw))
					CNs.addElement(JNTS);
			} else // check if accepted and also expand
			{
				if (acceptCondition(JNTS, allkeyw))
					CNs.addElement(JNTS);
				addVector(Q, pruneExpansions(getExpansions(JNTS, this), maxsize, allkeyw, sch));
			}
		}

		// 8/10/2001, remove duplicate CN's from CNs
		boolean moreIdenticalCNs = true;
		while (moreIdenticalCNs) {
			moreIdenticalCNs = false;
			label1: for (int i = 0; i < CNs.size(); i++)
				for (int j = 0; j < CNs.size(); j++)
					if (i != j)
						if (((Instance) CNs.elementAt(i)).identical((Instance) CNs.elementAt(j))) {
							CNs.removeElementAt(i);
							moreIdenticalCNs = true;
							break label1;
							// MessageBox.show("2 identical CNs","");//debug
						}
		}
		return CNs;
	}

	CNstats getCNsStats(int maxsize, Vector allkeyw, Schema sch) {// the same with getCNs, but keeps stats, instead of
																	// the CN's
																	// stats
		CNstats stats = new CNstats();
		// check if all keywords are present in schema
		if (!allkeywInSchema(allkeyw))
			return stats;
		int numActiveTSproduced = 0; // #JNTS put is Q
		int numTSwithKeyw = 0; // #JNTS put is Q, that have all keywords
		int numTSwithKeywNoLeaf = 0; // #JNTS put is Q, that have all keywords and no free tuple set as leaf
		int numCNs = 0; // #CN's
		// end stats
		// Vector CNs=new Vector(1);
		// find keyword in minimum # of relations
		String lfkeyw = findLeastFreqKeyw();
		if (lfkeyw == null)
			findLeastFreqKeyw();
		// add to Q all Instances(fresh copies with empty adjList) that contain lfkeyw
		Vector Q = getInstancesWithKeyw(lfkeyw); // queue of algorithm
		Q = allKeywComb4Instances(Q, lfkeyw);// adds all possible instances that contain lfkeyw
		while (!Q.isEmpty()) {
			Instance JNTS = (Instance) Q.firstElement(); // I is current active instance(joining net. of tuple sets)
			Q.removeElementAt(0);
			// stats
			stats.numActiveTSproduced++;
			boolean outpCN = false;
			if (!RedundantTS(JNTS)) // check if we can replace a TS with a free TS and still have all keywords
				if (JNTS.containsAllKeywords(allkeyw)) {
					stats.numTSwithKeyw++;
					if (!JNTS.hasFreeTSLeaf()) {
						stats.numTSwithKeywNoLeaf++;
						if (!pruningCondition(JNTS, sch)) {
							outpCN = true;
							stats.numCNs++;
						}
					}
				}
			if (!outpCN && JNTS.getsize() < maxsize)
				addVector(Q, getExpansions(JNTS, this));
			// end stats
			/*
			 * if (!pruningCondition(JNTS)) { if( acceptCondition(JNTS,allkeyw))
			 * CNs.addElement(JNTS); else if(JNTS.getsize()<maxsize) addVector(Q,
			 * getExpansions(JNTS, this)); }
			 */ }
		// return CNs;
		return stats;
	}

	void addPPpermut(Vector CNs) {// for each CN, add a CN for each permutation of directions between P-P
									// 3/28/2002 obsolete, since I fixed the getCN for many2many
		Vector newCNs = new Vector(1);// CNs+new
		for (int i = 0; i < CNs.size(); i++) {
			Instance CN = (Instance) ((Instance) CNs.elementAt(i)).clone();
			// find all Instance indexes in getAllInstances() and adjlist indexes pairs for
			// PP
			Vector instIndex = new Vector(1);
			Vector adjlistIndex = new Vector(1);
			Vector v = CN.getAllInstances();
			for (int j = 0; j < v.size(); j++) {
				Instance currInst = (Instance) v.elementAt(j);
				if (currInst.relationName.compareTo("P") == 0)
					for (int k = 0; k < currInst.adjList.size(); k++) {
						Instance cInst = (Instance) currInst.adjList.elementAt(k);
						if (cInst.relationName.compareTo("P") == 0
								&& ((String) currInst.incoming.elementAt(k)).compareTo("inc") == 0) // last condition is
																									// put to avoid
																									// duplicates
						{
							instIndex.addElement(new Integer(j));
							adjlistIndex.addElement(new Integer(k));
						}
					}
			}
			if (instIndex.isEmpty()) // no pp found
				newCNs.addElement(CN);
			else {
				Instance CN2 = (Instance) ((Instance) CNs.elementAt(i)).clone();
				Vector v2 = CN2.getAllInstances();
				for (int a = 0; a < Math.round(Math.pow(2, instIndex.size())); a++) {
					for (int h = 0; h < instIndex.size(); h++) {
						Instance in4 = (Instance) v2.elementAt(((Integer) instIndex.elementAt(h)).intValue());
						if ((a / Math.pow(2, h)) % 2 == 0) {
							in4.incoming.setElementAt("inc", ((Integer) adjlistIndex.elementAt(h)).intValue());
							Instance in4adj = (Instance) in4.adjList
									.elementAt(((Integer) adjlistIndex.elementAt(h)).intValue());
							for (int n = 0; n < in4adj.adjList.size(); n++)
								if (in4adj.adjList.elementAt(n).equals(in4))
									in4adj.incoming.setElementAt("out", n);
						} else {
							in4.incoming.setElementAt("out", ((Integer) adjlistIndex.elementAt(h)).intValue());
							Instance in4adj = (Instance) in4.adjList
									.elementAt(((Integer) adjlistIndex.elementAt(h)).intValue());
							for (int n = 0; n < in4adj.adjList.size(); n++)
								if (in4adj.adjList.elementAt(n).equals(in4))
									in4adj.incoming.setElementAt("inc", n);
						}
					}
					Instance CN3 = (Instance) CN2.clone();
					newCNs.addElement(CN3);
				}
			}
		}
		CNs.removeAllElements();
		for (int m = 0; m < newCNs.size(); m++)
			CNs.addElement(newCNs.elementAt(m));
	}

	void addKeywLeaves(Vector CNs) {// also sets the attr name of keyw tuple sets to "ID"
		for (int i = 0; i < CNs.size(); i++) {
			Vector v = ((Instance) CNs.elementAt(i)).getAllInstances();
			for (int j = 0; j < v.size(); j++) {
				Instance currInst = (Instance) v.elementAt(j);
				// 5/28/02 if all keyw in single tuple set, then do nothing, i.e., don't convert
				// A^{k1,k2} to A->A^{k1,k2}
				if (j == 0 && currInst.adjList.isEmpty())
					break;
				// end 5/28/02
				if (!currInst.keywords.isEmpty()) {
					Instance newInst = new Instance();
					newInst.relationName = currInst.relationName;
					for (int k = 0; k < currInst.keywords.size(); k++)
						newInst.addKeyword((String) currInst.keywords.elementAt(k));
					newInst.addAdjInstance(currInst, "out");
					newInst.addAttribute("ID");
					currInst.keywords.removeAllElements();
					currInst.addAdjInstance(newInst, "inc");
				}
			}
		}
	}

}
