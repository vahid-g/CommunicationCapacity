package irstyle_core;

import java.sql.*;
//import com.ms.wfc.ui.*;
import java.util.*;

public class JDBCaccess {
	private Statement stmt;
	public Connection conn;

	public JDBCaccess(String Server, String Port, String Database_name, String Username, String Password) {
		try {
			// DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			// conn =
			// DriverManager.getConnection ("jdbc:oracle:thin:@feast:1521:order1","vag",
			// "vag");
			// DriverManager.getConnection ("jdbc:oracle:thin:@vagelis:1521:vagdb","vag",
			// "vag");
			// DriverManager.getConnection("jdbc:mysql://" + Server + ":" + Port + "/" +
			// Database_name, Username,
			// Password); // jdbc\:mysql\://localhost\:3306/wikipedia
			Properties connectionProps = new Properties();
			connectionProps.put("user", Username);
			connectionProps.put("password", Password);
			conn = DriverManager.getConnection("jdbc:mysql://" + Server + ":3306/" + Database_name, connectionProps);
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			stmt = conn.createStatement();
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in JDBCaccess.JDBCaccess");
		}

	}

	public JDBCaccess(Connection conn) {
		try {
			this.conn = conn;
			stmt = conn.createStatement();
		} catch (Exception e1) {
			System.err.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage());
			e1.printStackTrace();
		}

	}

	public void execute(String sqlstatement) {
		try {
			stmt.execute(sqlstatement);
		} catch (Exception e1) {
			System.out.println("sql:" + sqlstatement + "  exception class: " + e1.getClass() + "  with message: "
					+ e1.getMessage() + "exception in  JDBCaccess.execute");
			e1.printStackTrace();
		}
	}

	ResultSet executeQuery(String sqlstatement) {
		try {
			return stmt.executeQuery(sqlstatement);
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.executeQuery");
			return null;
		}
	}

	boolean isTableEmpty(String tablename) {
		try {
			String sql = "SELECT count(*) FROM " + tablename;
			ResultSet rset = stmt.executeQuery(sql);
			rset.next();
			int count = rset.getInt(1);
			if (count > 0)
				return false;
			return true;
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.isTableEmpty");
			return false;
		}
	}

	boolean existsTable(String tablename) {
		try {

			String sql = "SELECT count(TNAME) FROM TAB where tname='" + tablename;
			sql += "'";
			sql += "   ";
			// sql=sql.substring(0,35+tablename.length() );
			ResultSet rset = stmt.executeQuery(sql);
			rset.next();
			int count = rset.getInt(1);
			if (count > 0)
				return true;
			return false;
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess. existsTable, table name=" + tablename);
			return false;
		}
	}

	int getTableSize(String tablename) {
		try {
			String sql = "SELECT count(*) FROM " + tablename;
			ResultSet rset = stmt.executeQuery(sql);
			rset.next();
			int count = rset.getInt(1);
			return count;
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.getTableSize");
			return 0;
		}
	}

	void closeall() {
		try {
			stmt.close();
			conn.close();
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in JDBCaccess.JDBCaccess");
		}
	}

	void start(String Server, String Port, String Database_name, String Username, String Password) {

		try {
			// DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
			conn =
					// DriverManager.getConnection ("jdbc:oracle:thin:@feast:1521:order1","vag",
					// "vag");
					// DriverManager.getConnection ("jdbc:oracle:thin:@vagelis:1521:vagdb","vag",
					// "vag");
					DriverManager.getConnection("jdbc:oracle:thin:@" + Server + ":" + Port + ":" + Database_name,
							Username, Password);
			stmt = conn.createStatement();
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in JDBCaccess.JDBCaccess");
		}
	}

	public int dropTable(String name) {
		String sql = "drop table if exists " + name;
		execute(sql);
		return 0;
	}

	PreparedStatement createPreparedStatement(String sql) {
		try {
			return conn.prepareStatement(sql);
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in createPreparedStatement: " + sql);
		}
		return null;
	}

	ResultSet executePrepared(PreparedStatement prepared) {
		try {
			return prepared.executeQuery();
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.executePrepared. prepared: " + prepared.toString());
			return null;
		}
	}

	void printResult(ResultSet rs) {
		System.out.println(getResult(rs));
	}

	String getResult(ResultSet rs) {
		try {
			String str = "";
			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			for (int i = 1; i <= numberOfColumns; i++) {
				String type = rsmd.getColumnTypeName(i);
				if (type.compareTo("INT") == 0)
					str += rs.getInt(i);
				else
					str += rs.getString(i);
				str += " - ";
			}
			return str;
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.printResult");
			return null;
		}
	}

	ResultSet createCursor(String sqlstatement) {
		try {
			Statement st = conn.createStatement();
			return st.executeQuery(sqlstatement);
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.createCursor");
			e1.printStackTrace();
			return null;
		}
	}

	int getNextID(ResultSet rset) {

		try {
			if (!rset.next()) // no more results
				return -1;
			return rset.getInt("id");
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ " exception in  JDBCaccess.getNextID");
			return -2;
		}
	}

	int getCurrScore(ResultSet rset) {

		try {
			return rset.getInt("SCORE");
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ " exception in  JDBCaccess.getCurrScore");
			return -2;
		}
	}

	int getTopNResults(String sql, int N) {
		try {
			long time1 = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(sql);
			int numresults = 0;
			while (numresults < N && rs.next()) {
				numresults++;
				this.printResult(rs);
			}
			long time2 = System.currentTimeMillis();
			return (int) (time2 - time1);
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.getTopNResults");
			e1.printStackTrace();
			return -1;
		}
	}

	public int getTopNResults(String sql, int N, ArrayList results) {// adds top-N results to the array results of
																		// Result type
		try {
			long time1 = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(sql);
			int numresults = 0;
			while (numresults < N && rs.next()) {
				numresults++;
				Result res = new Result(getResult(rs), rs.getDouble("totalscore"));
				results.add(res);
				// this.printResult(rs);
			}
			long time2 = System.currentTimeMillis();
			return (int) (time2 - time1);
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.getTopNResults2 sql:" + sql);
			e1.printStackTrace();
			return -1;
		}
	}

	boolean containsAll(String str, ArrayList keywords) {// true if str contains all strings in keywords
		boolean found[] = new boolean[keywords.size()];
		StringTokenizer st = new StringTokenizer(str);
		for (int i = 0; i < keywords.size(); i++)
			found[i] = false;
		while (st.hasMoreTokens()) {
			String s = st.nextToken();
			for (int i = 0; i < keywords.size(); i++) {
				if (s.compareToIgnoreCase((String) keywords.get(i)) == 0)
					found[i] = true;
			}
		}
		for (int i = 0; i < keywords.size(); i++)
			if (!found[i])
				return false;
		return true;
	}

	public int getTopNResultsAllKeyw(String sql, int N, ArrayList results, ArrayList keywords) {// adds top-N results to
																								// the
		// array results of Result
		// type
		try {
			long time1 = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(sql);
			int numresults = 0;
			while (numresults < N && rs.next()) {
				Result res = new Result(getResult(rs), rs.getDouble("totalscore"));
				if (containsAll(res.str, keywords)) {
					numresults++;
					results.add(res);
				}
				// this.printResult(rs);
			}
			long time2 = System.currentTimeMillis();
			return (int) (time2 - time1);
		} catch (Exception e1) {
			System.out.println("exception class: " + e1.getClass() + "  with message: " + e1.getMessage()
					+ "exception in  JDBCaccess.getTopNResults2 sql:" + sql);
			e1.printStackTrace();
			return -1;
		}
	}

}
