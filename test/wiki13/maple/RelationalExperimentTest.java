package wiki13.maple;

import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

public class RelationalExperimentTest {

    @Test
    public void testDatabaseConnection() throws IOException, SQLException {
	Properties config = new Properties();
	FileInputStream in = new FileInputStream("config.properties");
	config.load(in);
	in.close();
	java.sql.Connection conn = RelationalExperiment
		.getDatabaseConnection(config.get("username"),
			config.get("password"), config.get("db-url"));
	assertNotNull(conn);

	
    }

}
