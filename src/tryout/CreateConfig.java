package tryout;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class CreateConfig {
    public static void main(String[] args) throws IOException {
	FileOutputStream out = new FileOutputStream(
		"example-config.properties");
	Properties defaultProps = new Properties();
	defaultProps.setProperty("db-url",
		"jdbc:mysql://engr-db.engr.oregonstate.edu:port/db-name");
	defaultProps.setProperty("username", "your-username");
	defaultProps.setProperty("password", "****");
	defaultProps.store(out, "---Default Config Properties---");
	out.close();
    }
}
