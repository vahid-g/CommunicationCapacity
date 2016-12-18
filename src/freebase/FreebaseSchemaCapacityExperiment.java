package freebase;

import java.io.FileWriter;
import java.util.List;

public class FreebaseSchemaCapacityExperiment {

	/**
	 * schema capacity experiment. queries selected based on specific set of
	 * keywords and submitted to a smaller table with keywords dropped
	 * 
	 * @param tableName
	 * @param pattern
	 */
	public static void schemaCapacitySmallerTable(String tableName, String pattern) {
		// String[] table = { "tbl_tv_program", "tbl_album", "tbl_book" };
		// String[] pattern = {
		// "program| tv| television| serie| show | show$| film | film$| movie",
		// "music|record|song|sound| art |album",
		// "book|theme|novel|notes|writing|manuscript|story" };

		String attribs[] = { FreebaseDataManager.NAME_ATTRIB, FreebaseDataManager.DESC_ATTRIB };
		String indexPath = FreebaseDatabaseSizeExperiment.INDEX_BASE + tableName + "/";
//		createIndex(tableName, attribs, indexPath);
		String sql = "select * from query where text REGEXP '" + pattern + "' and fbid in (select fbid from "
				+ tableName + ");";
		List<FreebaseQuery> queries = FreebaseDataManager.getQueriesBySqlQuery(sql);
		FreebaseDataManager.removeKeyword(queries, pattern);
		try (FileWriter fw = new FileWriter(
				FreebaseDatabaseSizeExperiment.RESULT_DIR + "t-" + tableName + " q-" + tableName + " a-name" + ".csv");) {
			for (FreebaseQuery query : queries) {
				FreebaseDataManager.runQuery(query, indexPath);
				fw.write(query.id + ", " + query.text + ", " + query.wiki + ", " + query.p3() + ", "
						+ query.precisionAtK(10) + ", " + query.precisionAtK(20) + ", " + query.mrr() + ","
						+ query.hits[0] + ", " + query.hits[1] + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * schema capacity experiment. queries selected based on specific set of
	 * keywords and submitted to a larger table. Note that the keywords are not
	 * dropped.
	 * 
	 * @param tableName
	 * @param pattern
	 * @param queryTableName
	 */
	public static void schemaCapacityLargerTable(String tableName, String pattern, String queryTableName) {
		String attribs[] = { FreebaseDataManager.NAME_ATTRIB, FreebaseDataManager.DESC_ATTRIB,
				FreebaseDataManager.SEMANTIC_TYPE_ATTRIB };
		String indexPath = FreebaseDatabaseSizeExperiment.INDEX_BASE + tableName + "/";
//		createIndex(tableName, attribs, indexPath);
		String sql = "select * from query where text REGEXP '" + pattern + "' and fbid in (select fbid from "
				+ queryTableName + ");";
		List<FreebaseQuery> queries = FreebaseDataManager.getQueriesBySqlQuery(sql);
		FreebaseDataManager.annotateSemanticType(queries, pattern);
		try (FileWriter fw = new FileWriter(
				FreebaseDatabaseSizeExperiment.RESULT_DIR + "t-" + tableName + " q-" + queryTableName + " a-name" + ".csv");) {
			for (FreebaseQuery query : queries) {
				FreebaseDataManager.runQuery(query, indexPath);
				fw.write(query.id + ", " + query.text + ", " + query.wiki + ", " + query.p3() + ", "
						+ query.precisionAtK(10) + ", " + query.precisionAtK(20) + ", " + query.mrr() + ","
						+ query.hits[0] + ", " + query.hits[1] + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
