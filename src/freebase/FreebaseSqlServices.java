package freebase;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;

public class FreebaseSqlServices {

	public static void deleteTable(String tableName) {
		Statement stmt = null;
		try (Connection databaseConnection = FreebaseDataManager.getDatabaseConnection()) {
			stmt = databaseConnection.createStatement();
			stmt.executeUpdate("delete table " + tableName);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

	public static void createSampledTables(String tableName, int tableNumber) {
		Statement st = null;
		String newName = tableName;
		try (Connection conn = FreebaseDataManager.getDatabaseConnection()) {
			st = conn.createStatement();
			for (int i = 1; i <= tableNumber; i++) {
				String sql = "create table " + newName + "_" + i
						+ " as select * from " + tableName + " where mod("
						+ tableName + ".counter, " + tableNumber + ") < " + i
						+ ";";
				st.executeUpdate(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (st != null)
					st.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
		}
	}

	public static void createShuffledTable(String tableName, String newTableName) {
		Statement stmt = null;
		try (Connection databaseConnection = FreebaseDataManager.getDatabaseConnection()) {
			stmt = databaseConnection.createStatement();
			stmt.executeUpdate(
					"create table " + newTableName + " as select * from table " + tableName + " order by rand()");
		} catch (Exception e) {
			FreebaseDataManager.LOGGER.log(Level.SEVERE, e.toString());
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se) {
				FreebaseDataManager.LOGGER.log(Level.SEVERE, se.toString());
			}
		}
	}

}
