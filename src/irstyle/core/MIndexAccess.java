package irstyle.core;
//package xkeyword;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

public class MIndexAccess {
	Vector tuplesets;
	Random ran = new Random();
	Vector relations;

	MIndexAccess() {
		tuplesets = new Vector(1);
	}

	public MIndexAccess(Vector r) {
		relations = r;
		tuplesets = new Vector(1);
	}

	private class TupleSet {
		String relname;
		String TSname;
		Vector keywords;
	}

	boolean alphaorder(Vector v) {
		for (int i = 0; i < v.size() - 1; i++)
			if (((String) v.elementAt(i)).compareTo((String) v.elementAt(i + 1)) > 0)
				return false;
		return true;
	}

	private Vector getKeywCombinations(Vector keywV) {// returns a Vector of String Vectors, one for each combination of
														// strings in keywV
														// only combinations in alphabetical order are returned to avoid
														// duplicates
		try {
			Vector ret = new Vector(1);
			int numCombs = (int) Math.round(Math.pow(2, keywV.size()));
			for (int i = 0; i < numCombs; i++) {
				Vector oneComb = new Vector(1);
				for (int keywIndex = 0; keywIndex < keywV.size(); keywIndex++)
					if (i / (Math.round(Math.pow(2, keywIndex))) % 2 == 1)
						oneComb.addElement(keywV.elementAt(keywIndex));
				if (!oneComb.isEmpty() /* && alphaorder(oneComb) */)
					ret.addElement(oneComb);
			}
			return ret;

		} catch (Exception e1) {
			// MessageBox.show("exception class: " + e1.getClass() +" with message: " +
			// e1.getMessage(),"exception in getKeywCombinations");
			return null;
		}
	}

	public boolean hasTextAttr(Relation rel) {
		for (int i = 0; i < rel.getNumAttributes(); i++)
			if (rel.isInMasterIndex(rel.getAttribute(i)))
				return true;
		return false;
	}

	void createTupleSets(Schema sch, Vector allkeywords, Connection conn) {
		JDBCaccess jdbcacc = new JDBCaccess(conn);
		// create non-empty tuple sets and add keywords to schema
		// tuple set is named R_keyw. eg: C_SIGMOD
		Vector allInst = sch.getAllInstances();
		// Vector keywcombs=getKeywCombinations( allkeyw);
		String keywList = "";
		for (int j = 0; j < allkeywords.size(); j++) {
			String keyw = (String) allkeywords.elementAt(j);
			keywList += keyw + ",";
		}
		keywList = keywList.substring(0, keywList.length() - 1);

		for (int i = 0; i < relations.size(); i++)
			if (hasTextAttr((Relation) relations.elementAt(i))) {
				String orderbyclause = "";
				Relation rel = (Relation) relations.elementAt(i);
				String startOfCommand = "CREATE TABLE TS_" + rel.getName() + " AS SELECT id,title,text, ";
				String columns = "";
				for (int k = 0; k < rel.getNumAttributes(); k++) {
					if (rel.isInMasterIndex(rel.getAttribute(k))) {
						if (!columns.equals("")) {
							columns += ",";
						}
						columns += rel.getAttribute(k);
					}
				}
				String command = "match(" + columns + ") against('" + keywList + "' IN BOOLEAN MODE)";
				// command+=storage_clause;
				command = startOfCommand + command + " as score FROM " + rel.getName() + " order by score desc;";
				// cleanupCommands.addElement( (String) ("DROP TABLE TS_"+rel.getName()));
				jdbcacc.execute(command);// SQLcommands.addElement(command);
				if (Flags.DEBUG_INFO2)
					System.out.println(command);
				if (!jdbcacc.isTableEmpty("TS_" + rel.getName())) {// add all or none keywords
					for (int y = 0; y < allkeywords.size(); y++)
						sch.getInstance(rel.getName()).addKeyword((String) allkeywords.elementAt(y));
					TupleSet ts = new TupleSet();
					ts.relname = rel.getName();
					ts.TSname = "TS_" + rel.getName();
					ts.keywords = (Vector) allkeywords.clone();
					tuplesets.addElement(ts);
				}
			}

	}

	public void createTupleSets2(Schema sch, Vector allkeywords, Connection conn) {
		JDBCaccess jdbcacc = new JDBCaccess(conn);
		// create non-empty tuple sets and add keywords to schema
		// tuple set is named R_keyw. eg: C_SIGMOD
		Vector allInst = sch.getAllInstances();
		// Vector keywcombs=getKeywCombinations( allkeyw);
		String keywList = "";
		for (int j = 0; j < allkeywords.size(); j++) {
			String keyw = (String) allkeywords.elementAt(j);
			keywList += keyw + " ";
		}
		keywList = keywList.substring(0, keywList.length() - 1);

		for (int i = 0; i < relations.size(); i++)
			if (hasTextAttr((Relation) relations.elementAt(i))) {
				String orderbyclause = "";
				Relation rel = (Relation) relations.elementAt(i);
				String startOfCommand = "CREATE TABLE TS_" + rel.getName() + " AS SELECT ";
				for (int j = 0; j < rel.getAttributes().size(); j++) {
					// if (rel.isInMasterIndex(rel.getAttribute(j))) {
					startOfCommand += rel.getAttribute(j) + ", ";
					// }
				}
				String columns = "";
				for (int k = 0; k < rel.getNumAttributes(); k++) {
					if (rel.isInMasterIndex(rel.getAttribute(k))) {
						if (!columns.equals("")) {
							columns += ",";
						}
						columns += rel.getAttribute(k);
					}
				}
				String command = "";
				String matchAgainst = "match(" + columns + ") against('" + keywList + "' IN NATURAL LANGUAGE MODE)";
				// command+=storage_clause;
				command = startOfCommand + matchAgainst + " as score FROM " + rel.getName() + " where " + matchAgainst
						+ ";";
				// cleanupCommands.addElement( (String) ("DROP TABLE TS_"+rel.getName()));
				jdbcacc.execute(command);// SQLcommands.addElement(command);
				if (Flags.DEBUG_INFO2)
					System.out.println(command);
				if (!jdbcacc.isTableEmpty("TS_" + rel.getName())) {// add all or none keywords
					for (int y = 0; y < allkeywords.size(); y++)
						sch.getInstance(rel.getName()).addKeyword((String) allkeywords.elementAt(y));
					TupleSet ts = new TupleSet();
					ts.relname = rel.getName();
					ts.TSname = "TS_" + rel.getName();
					ts.keywords = (Vector) allkeywords.clone();
					tuplesets.addElement(ts);
				}
			}

	}

	public void createTupleSets3(Schema sch, Vector allkeywords, Connection conn,
			Map<String, List<String>> relnameValues) {
		JDBCaccess jdbcacc = new JDBCaccess(conn);
		// create non-empty tuple sets and add keywords to schema
		Vector allInst = sch.getAllInstances();
		String keywList = "";
		for (int j = 0; j < allkeywords.size(); j++) {
			String keyw = (String) allkeywords.elementAt(j);
			keywList += keyw + " ";
		}
		keywList = keywList.substring(0, keywList.length() - 1);
		for (int i = 0; i < relations.size(); i++) {
			if (hasTextAttr((Relation) relations.elementAt(i))) {
				Relation rel = (Relation) relations.elementAt(i);
				String tuplesetName = "TS_" + rel.getName();
				String createTable = "CREATE TABLE  " + tuplesetName + "(id int, score int);";
				jdbcacc.execute(createTable);
				List<String> values = relnameValues.get(rel.name);
				for (String value : values) {
					String insertInto = "INSERT INTO " + tuplesetName + "(id, score) VALUES " + value + ";";
					jdbcacc.execute(insertInto);
				}
				if (!jdbcacc.isTableEmpty("TS_" + rel.getName())) {// add all or none keywords
					for (int y = 0; y < allkeywords.size(); y++)
						sch.getInstance(rel.getName()).addKeyword((String) allkeywords.elementAt(y));
					TupleSet ts = new TupleSet();
					ts.relname = rel.getName();
					ts.TSname = "TS_" + rel.getName();
					ts.keywords = (Vector) allkeywords.clone();
					tuplesets.addElement(ts);
				}
			}
		}
	}

	public void createTupleSetsFast(Schema sch, Vector allkeywords, Connection conn,
			Map<String, List<String>> relnameValues) throws SQLException {
		JDBCaccess jdbcacc = new JDBCaccess(conn);
		// create non-empty tuple sets and add keywords to schema
		Vector allInst = sch.getAllInstances();
		String keywList = "";
		for (int j = 0; j < allkeywords.size(); j++) {
			String keyw = (String) allkeywords.elementAt(j);
			keywList += keyw + " ";
		}
		keywList = keywList.substring(0, keywList.length() - 1);
		conn.setAutoCommit(false);
		for (int i = 0; i < relations.size(); i++) {
			if (hasTextAttr((Relation) relations.elementAt(i))) {
				Relation rel = (Relation) relations.elementAt(i);
				String tuplesetName = "TS_" + rel.getName();
				String createTable = "CREATE TABLE  " + tuplesetName + "(id int, score float);";
				jdbcacc.execute(createTable);
				List<String> values = relnameValues.get(rel.name);
				String insertIntoTemplate = "INSERT INTO " + tuplesetName + "(id, score) VALUES (?,?);";
				PreparedStatement stmt = jdbcacc.createPreparedStatement(insertIntoTemplate);
				for (String value : values) {
					String[] fields = value.split(",");
					stmt.setInt(1, Integer.parseInt(fields[0].substring(1)));
					stmt.setFloat(2, Float.parseFloat(fields[1].substring(0, fields[1].length() - 1)));
					stmt.addBatch();
				}
				stmt.executeBatch();
				conn.commit();
				if (!jdbcacc.isTableEmpty("TS_" + rel.getName())) {// add all or none keywords
					for (int y = 0; y < allkeywords.size(); y++)
						sch.getInstance(rel.getName()).addKeyword((String) allkeywords.elementAt(y));
					TupleSet ts = new TupleSet();
					ts.relname = rel.getName();
					ts.TSname = "TS_" + rel.getName();
					ts.keywords = (Vector) allkeywords.clone();
					tuplesets.addElement(ts);
				}
			}
		}
		conn.setAutoCommit(true);
	}

	void clearTupleSets(Connection conn) {
		JDBCaccess jdbcacc = new JDBCaccess(conn);
		for (int i = 0; i < tuplesets.size(); i++) {
			jdbcacc.dropTable((String) ((TupleSet) tuplesets.elementAt(i)).TSname);
		}
	}

	protected boolean stringContained(Vector strV, String str) {// checks if str is in String Vector strV
		for (int i = 0; i < strV.size(); i++)
			if (str.compareTo((String) strV.elementAt(i)) == 0)
				return true;
		return false;
	}

	private String getrelname4keywRel(Vector keywords, String relationName) {
		for (int i = 0; i < tuplesets.size(); i++) {
			TupleSet ts = (TupleSet) tuplesets.elementAt(i);
			if (relationName.compareTo(ts.relname) == 0 && keywords.size() == ts.keywords.size()) {
				boolean equal = true;
				for (int j = 0; j < keywords.size(); j++)
					if (!stringContained(keywords, (String) ts.keywords.elementAt(j)))
						equal = false;
				if (equal)
					return ts.TSname;
			}
		}
		return null;
	}

	void UpdateRelNames(Vector CNs) {
		for (int i = 0; i < CNs.size(); i++) {
			Instance inst = (Instance) CNs.elementAt(i);
			Vector v = inst.getAllInstances();
			for (int j = 0; j < v.size(); j++) {
				Instance in = (Instance) v.elementAt(j);
				if (!in.keywords.isEmpty())
					in.relationName = getrelname4keywRel(in.keywords, in.relationName);

			}
		}
	}

	private boolean EqualVectors(Vector v1, Vector v2) {
		// true in v1,v2 contain same strings. We assume that no vector contains
		// duplicates
		if (v1.size() != v2.size())
			return false;
		for (int i = 0; i < v1.size(); i++)
			if (!stringContained(v2, (String) v1.elementAt(i)))
				return false;
		return true;
	}

	Vector RemoveCNswEmptyTS(Vector CNs) {// remove all CNs with tuple sets of more than 1 keyw, that don't exist in
											// tuplesets
		Vector newCNs = new Vector(1);
		for (int i = 0; i < CNs.size(); i++) {
			Instance inst = (Instance) ((Instance) CNs.elementAt(i)).clone();
			boolean accepted = true;
			Vector v = inst.getAllInstances();
			for (int j = 0; j < v.size(); j++) {
				Instance in2 = (Instance) v.elementAt(j);
				if (in2.keywords.size() > 1) {
					boolean found = false;
					for (int l = 0; l < tuplesets.size(); l++) {
						TupleSet ts = (TupleSet) tuplesets.elementAt(l);
						if (ts.relname.compareTo(in2.relationName) == 0 && EqualVectors(ts.keywords, in2.keywords))
							found = true;
					}
					if (!found)
						accepted = false;
				}
			}
			if (accepted)
				newCNs.addElement(inst);
		}
		return newCNs;// CNs=(Vector) newCNs.clone();
	}

	Vector getKeywComb4Relname(String rname) {
		Vector v = new Vector(1);
		v.addElement(new Vector(0));
		for (int i = 0; i < tuplesets.size(); i++) {
			TupleSet t = (TupleSet) tuplesets.elementAt(i);
			if (t.relname.compareTo(rname) == 0)
				v.addElement(t.keywords);
		}
		return v;
	}
}
