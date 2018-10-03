package irstyle.api;

import java.sql.SQLException;
import java.util.Vector;

import irstyle.core.JDBCaccess;
import irstyle.core.Relation;

public abstract class IRStyleExperimentHelper {

	protected JDBCaccess jdbcAccess;

	public JDBCaccess getJdbcAccess() {
		return jdbcAccess;
	}

	abstract public Vector<Relation> createRelations(String firstTable, String secondTable, String thirdTable,
			String firstRel, String secondRel) throws SQLException;

}
