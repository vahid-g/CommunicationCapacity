package wiki13.maple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

public class RelationalExperimentTest {

    @Test
    public void testDatabaseConnection() throws IOException, SQLException {
	Properties config = new Properties();
	FileInputStream in = new FileInputStream("config.properties");
	config.load(in);
	in.close();
	java.sql.Connection conn = RelationalExperiment.getDatabaseConnection(
		config.get("username"), config.get("password"),
		config.get("db-url"));
	assertNotNull(conn);

	List<String> ids = new ArrayList<String>();
	ids.add("10483000");
	String query = "SELECT a.id FROM tbl_article_wiki13 a left join "
		+ "tbl_article_image_09 i on a.id = i.article_id left join "
		+ "tbl_article_link_09 l on a.id=l.article_id WHERE a.id in (10483000)";
	List<String> results = RelationalExperiment.submitSqlQuery(conn, query);
	assertEquals(17, results.size());
    }

}
