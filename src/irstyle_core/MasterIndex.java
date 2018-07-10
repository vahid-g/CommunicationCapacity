package irstyle_core;

import java.util.*;

import irstyle.IRStyleMain;
import irstyle.IRStyleParams;

public class MasterIndex {/*
							 * We use Oracle8i interMedia Text 8.1.5 to create the invetred index for each
							 * attribute. This class creates the script to create these indices and uses
							 * them to create in KEEP buffer pool the tuple sets
							 */

	public MasterIndex() {

	}

	public String createInterMediaScript(Vector relations) {// index name must have <=25 chars
		String script = "";
		for (int i = 0; i < relations.size(); i++) {
			Relation rel = (Relation) relations.elementAt(i);
			for (int j = 0; j < rel.getNumAttributes(); j++)
				if (rel.isInMasterIndex(rel.getAttribute(j)))
					script += "create index " + rel.getName() + rel.getAttribute(j) + " on " + rel.getName() + "("
							+ rel.getAttribute(j) + ") indextype is ctxsys.context; \n ";
		}
		return script;
	}

	private boolean hasTextAttr(Relation rel) {
		for (int i = 0; i < rel.getNumAttributes(); i++)
			if (rel.isInMasterIndex(rel.getAttribute(i)))
				return true;
		return false;
	}

	Vector createTupleSetsScript(Vector relations, Vector allkeywords, Vector cleanupCommands, Vector tupleSetNames,
			Vector tupleSetInfoVect) {// returns a Vector of Strings, one String for each SQL command needed to create
										// the tuple sets
										// for each relation R in relations and for each non-empty combination
										// K={k1,..,kn} of keywords in allkeywords
										// it creates a table in KEEP buffer pool that has the rows in R that contain
										// exactly keywords in K
										// and no more.
										// The name of such a table is R_1_..._n, where 1,..,n the indices of the
										// corresponding keywords
										// in allkeywords

		// for each relation R
		// first for each keyword k create a temporary table that contains all rows that
		// have keyword k
		// called basic tuple sets in paper.
		// name for this table is "temp_R_i", where i is keyword's index;

		// tupleSetNames will have names of tables of the tuple sets
		// tupleSetInfoVect will have a TupleSetInfo obj for each tuple set

		// NOTE: leave KEEP for now because it is a syntax error to put it in a create
		// table statement
		// with an AS subquery clause.
		// cleanupCommands drops all tuple set tables
		// String storage_clause=" STORAGE (BUFFER_POOL KEEP)";
		Vector SQLcommands = new Vector(1);
		// cleanupCommands=new Vector(1);
		String storage_clause = "";

		for (int i = 0; i < relations.size(); i++)
			if (hasTextAttr((Relation) relations.elementAt(i))) {
				Relation rel = (Relation) relations.elementAt(i);
				String command = "CREATE TABLE TS_" + rel.getName() + " AS SELECT * FROM " + rel.getName() + " WHERE ";
				boolean first = true;
				for (int j = 0; j < allkeywords.size(); j++) {
					String keyw = (String) allkeywords.elementAt(j);

					for (int k = 0; k < rel.getNumAttributes(); k++)
						if (rel.isInMasterIndex(rel.getAttribute(k))) {
							if (!first)
								command += " OR ";
							if (first)
								first = false;
							command += "contains(" + rel.getAttribute(k) + ",'" + keyw + "')>0 ";
						}
					command += storage_clause;

					// tupleSetNames.addElement("nothing"); //10/12/01
					// tupleSetInfoVect.addElement(new TupleSetInfo("nothing",null));
				}
				cleanupCommands.addElement((String) ("DROP TABLE TS_" + rel.getName()));
				SQLcommands.addElement(command);
			}

		// create tuple sets from basic tuple sets
		/*
		 * Vector allkeywComb= (new Instance()).getKeywCombinations(allkeywords);
		 * for(int k=0;k<allkeywComb.size();k++) { Vector keywords=(Vector)
		 * allkeywComb.elementAt(k); if(!keywords.isEmpty()) { String
		 * tablename=rel.getName(); for(int y=0;y<keywords.size();y++)
		 * tablename+="_"+getIndex((String) keywords.elementAt(y),allkeywords);
		 * cleanupCommands.addElement("DROP TABLE "+tablename); String
		 * command="CREATE TABLE "+tablename+" AS ( ("; for(int
		 * y=0;y<keywords.size();y++) { if(y!=0) command+=" INTERSECT ";
		 * command+=" SELECT * FROM temp_"+rel.getName()+"_"+getIndex((String)
		 * keywords.elementAt(y),allkeywords); } command+=") "; for(int
		 * y=0;y<allkeywords.size();y++) if(!(new Instance()).stringContained(keywords,
		 * (String) allkeywords.elementAt(y)))
		 * command+=" MINUS SELECT * FROM temp_"+rel.getName()+"_"+y; //union,intersect
		 * command+=" )"; SQLcommands.addElement(command);
		 * tupleSetNames.addElement(tablename); //10/12/01
		 * tupleSetInfoVect.addElement(new TupleSetInfo(rel.getName(),keywords)); } }
		 * 
		 * //drop basic tuple sets. Moved it below to facilitate the experiment in
		 * MainThread //for(int j=0;j<allkeywords.size();j++) // SQLcommands.addElement(
		 * (String) ("DROP TABLE temp_"+rel.getName()+"_"+j)); } //drop basic tuple sets
		 * for(int i=0;i<relations.size();i++) { Relation rel=(Relation)
		 * relations.elementAt(i); for(int j=0;j<allkeywords.size();j++)
		 * SQLcommands.addElement( (String) ("DROP TABLE temp_"+rel.getName()+"_"+j)); }
		 */
		return SQLcommands;
	}

	int getIndex(String str, Vector strVect) {
		for (int i = 0; i < strVect.size(); i++)
			if (((String) strVect.elementAt(i)).compareTo(str) == 0)
				return i;
		return -1;
	}
}
