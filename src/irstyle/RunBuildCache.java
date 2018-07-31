package irstyle;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import database.DatabaseConnection;
import database.DatabaseType;

public class RunBuildCache {
	public static void main(String[] args) throws SQLException, IOException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			if (args.length < 3) {
				System.out.println("Usage: \n\t java -jar runbuildcache.jar table_name percent text_attribs\n\t"
						+ " java -jar runbuildcache.jar tbl_article_09 10 title,text");
			}
			String tableName = args[0];
			int percent = Integer.parseInt(args[1]);
			String textAttrib = args[2];
			int limit = (DatabaseHelper.tableSize(tableName, dc.getConnection()) * percent) / 100;
			String selectStatement = "SELECT * FROM " + tableName + " ORDER BY popularity LIMIT " + limit;
			String newTableName = "sub_" + tableName.substring(4) + "_" + percent;
			String createStatement = "CREATE TABLE " + newTableName + " AS " + selectStatement + ";";
			System.out.println("Creating table..");
			try (Statement stmt = dc.getConnection().createStatement()) {
				stmt.execute(createStatement);
			}
			System.out.println("Creating id index..");
			String createIndex = "CREATE INDEX id ON " + newTableName + "(id);";
			try (Statement stmt = dc.getConnection().createStatement()) {
				stmt.executeUpdate(createIndex);
			}
			System.out.println("Creating fulltext index..");
			String createFulltextIndex = "CREATE FULLTEXT INDEX fdx ON " + newTableName + "(" + textAttrib + ")";
			try (Statement stmt = dc.getConnection().createStatement()) {
				stmt.executeUpdate(createFulltextIndex);
			}
		}
	}
}
