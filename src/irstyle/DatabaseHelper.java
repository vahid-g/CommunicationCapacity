package irstyle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseHelper {

	static int tableSize(String tableName, Connection conn) throws SQLException {
		int count = -1;
		try (Statement stmt = conn.createStatement()) {
			String sql = "select count(*) count from " + tableName + ";";
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				count = rs.getInt("count");
			}
		}
		return count;
	}

}
