package tryout;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class General {

	static final Logger LOGGER = LogManager.getLogManager().getLogger("");

	public static void main(String[] args) throws IOException {
		Float[] nullArray = null;
		System.out.println(Arrays.stream(nullArray).reduce(Float::sum));
	}
	
	public static void testDivision() {
		int a = 1;
		int b = 2;
		System.out.println(a / b);
		System.out.println(a / (double) b);
	}

	public static void testLogger() {
		LOGGER.setLevel(Level.FINE);
		for (Handler h : LOGGER.getHandlers()) {
			h.setLevel(Level.FINE);
		}
		LOGGER.log(Level.INFO, "hanhan");
		LOGGER.log(Level.FINE, "olde?");
	}

	public static void createConfig() throws IOException {
		FileOutputStream out = new FileOutputStream("example-config.properties");
		Properties defaultProps = new Properties();
		defaultProps.setProperty("db-url", "jdbc:mysql://engr-db.engr.oregonstate.edu:port/db-name");
		defaultProps.setProperty("username", "your-username");
		defaultProps.setProperty("password", "****");
		defaultProps.store(out, "---Default Config Properties---");
		out.close();
	}

}
