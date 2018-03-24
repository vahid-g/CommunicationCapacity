package database;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import wiki13.maple.WikiMapleRelationalEfficiencyExperiment;

public class DatabaseConnection {

	private static final Logger LOGGER = Logger.getLogger(DatabaseConnection.class.getName());

	private Properties config;

	private Connection connection;

	public DatabaseConnection(DatabaseType databaseType) throws IOException, SQLException {
		config = new Properties();
		try (InputStream in = WikiMapleRelationalEfficiencyExperiment.class
				.getResourceAsStream("/config/config.properties")) {
			config.load(in);
		}
		createDatabaseConnection(config.getProperty(databaseType.getType()));
	}

	protected void createDatabaseConnection(String databasename) throws SQLException {
		Properties connectionProps = new Properties();
		connectionProps.put("user", config.get("username"));
		connectionProps.put("password", config.get("password"));
		try {
			connection = DriverManager.getConnection(databasename, connectionProps);
		} catch (SQLException e) {
			throw e;
		}
		LOGGER.log(Level.INFO, "Successfully connected to db");
	}

	public Connection getConnection() {
		return connection;
	}

	public void closeConnection() throws Throwable {
		if (connection != null) {
			LOGGER.log(Level.INFO, "Closing the databse connection..");
			connection.close();
		}
		super.finalize();
	}

}
