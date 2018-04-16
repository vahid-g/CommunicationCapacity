package tryout;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.index.IndexWriterConfig;

public class General {

	static final Logger LOGGER = LogManager.getLogManager().getLogger("");

	public static void main(String[] args) throws IOException {
		IndexWriterConfig iwc = new IndexWriterConfig();
		System.out.println(iwc.getMaxBufferedDocs());
		System.out.println(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
		System.out.println(IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS);
	}

	static void testDivision() {
		int a = 1;
		int b = 2;
		System.out.println(a / b);
		System.out.println(a / (double) b);
	}

	static void testLogger() {
		LOGGER.setLevel(Level.FINE);
		for (Handler h : LOGGER.getHandlers()) {
			h.setLevel(Level.FINE);
		}
		LOGGER.log(Level.INFO, "hanhan");
		LOGGER.log(Level.FINE, "olde?");
	}

	static void createConfig() throws IOException {
		FileOutputStream out = new FileOutputStream("example-config.properties");
		Properties defaultProps = new Properties();
		defaultProps.setProperty("db-url", "jdbc:mysql://engr-db.engr.oregonstate.edu:port/db-name");
		defaultProps.setProperty("username", "your-username");
		defaultProps.setProperty("password", "****");
		defaultProps.store(out, "---Default Config Properties---");
		out.close();
	}

}
