package stackoverflow.irstyle;

import java.io.IOException;
import java.sql.SQLException;

import database.DatabaseConnection;
import database.DatabaseType;
import irstyle.DatabaseHelper;
import irstyle.api.Indexer;

public class TableIndexer {

	public static final String ID_FIELD = "id";
	public static final String TEXT_FIELD = "text";
	public static final String WEIGHT_FIELD = "weight";

	public static void main(String[] args) throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW)) {
			for (int i = 0; i < Constants.tableName.length; i++) {
				String tableName = Constants.tableName[i];
				String[] textAttribs = Constants.textAttribs[i];
				String popularity = "ViewCount";
				int limit = DatabaseHelper.tableSize(tableName, dc.getConnection());
				String indexPath = Constants.DATA_STACK + tableName + "/100";
				Indexer.indexTable(dc, indexPath, tableName, textAttribs, limit, popularity, false,
						Indexer.getIndexWriterConfig());
			}
		}
	}
}
