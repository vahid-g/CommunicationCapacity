package stackoverflow.irstyle;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import database.DatabaseConnection;
import database.DatabaseType;
import irstyle.api.DatabaseHelper;
import irstyle.api.Indexer;

public class TableIndexer {

	public static final String ID_FIELD = "id";
	public static final String TEXT_FIELD = "text";
	public static final String WEIGHT_FIELD = "weight";

	public static void main(String[] args) throws IOException, SQLException {
		List<String> argList = Arrays.asList(args);
		boolean useCache = false;
		if (argList.contains("-cacahe")) {
			useCache = true;
		}
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW)) {
			for (int i = 0; i < StackConstants.tableName.length; i++) {
				String tableName = StackConstants.tableName[i];
				String[] textAttribs = StackConstants.textAttribs[i];
				String popularity = "ViewCount";
				int limit;
				if (useCache) {
					limit = StackConstants.cacheSize[i];
				} else {
					limit = DatabaseHelper.tableSize(tableName, dc.getConnection());
				}
				String indexPath = StackConstants.DATA_STACK + tableName + "_full";
				IndexWriterConfig config = Indexer.getIndexWriterConfig();
				config.setOpenMode(OpenMode.CREATE);
				Indexer.indexTable(dc, indexPath, tableName, textAttribs, limit, popularity, false, config);
			}
		}
	}
}
