package irstyle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

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

	static int relCounts(Connection conn, Set<String> ids) throws SQLException {
		int count = 0;
		String sql = " select count(*) as count from tbl_article_wiki13 a join tbl_article_image_09 ai on"
				+ " a.id = ai.article_id join tbl_article_link_09 al on a.id = al.article_id" + " where a.id = ?;";
		try (PreparedStatement stmt = conn.prepareStatement(sql)) {
			for (String id : ids) {
				stmt.setInt(1, Integer.parseInt(id));
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					count += rs.getInt("count");
				}
			}
		}
		return count;
	}

}
