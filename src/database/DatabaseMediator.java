package database;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import wiki13.maple.WikiMapleRelationalEfficiencyExperiment;

public class DatabaseMediator {

	private static final Logger LOGGER = Logger.getLogger(DatabaseMediator.class.getName());

	private Properties config;

	private Connection conn;

	public DatabaseMediator(DatabaseType databseType) throws IOException, SQLException {
		try (InputStream in = WikiMapleRelationalEfficiencyExperiment.class
				.getResourceAsStream("/config/config.properties")) {
			config.load(in);
		}
		createDatabaseConnection(config.getProperty(databseType.getType()));
	}

	public void createConnectionForStackoverflow() throws SQLException {
		createDatabaseConnection(config.getProperty("stack-db"));
	}

	private void createDatabaseConnection(String databasename) throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", config.get("username"));
		connectionProps.put("password", config.get("password"));
		try {
			conn = DriverManager.getConnection(databasename, connectionProps);
		} catch (SQLException e) {
			throw e;
		}
		LOGGER.log(Level.INFO, "Successfully connected to db");
	}

	@Override
	protected void finalize() throws Throwable {
		if (conn != null) {
			LOGGER.log(Level.INFO, "Closing the databse connection..");
			conn.close();
		}
		super.finalize();
	}

	public long submitSqlQuery(String query) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			long begin = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(query);
			long end = System.currentTimeMillis();
			int counter = 0;
			while (rs.next()) {
				counter++;
				rs.getString("id");
			}
			LOGGER.log(Level.FINE, "fetch size: " + counter);
			return end - begin;
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return -1;
	}

}
