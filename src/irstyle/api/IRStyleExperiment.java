package irstyle.api;

import database.DatabaseType;
import irstyle.Stack_Constants;
import irstyle.Wiki_Constants;

public class IRStyleExperiment {

	public final String[] tableNames;

	public final String[][] textAttribs;

	public final String popularity;

	public final String[] cacheNames;

	public final int[] limits;

	public final int[] sizes;

	public final String dataDir;

	public final DatabaseType databaseType;

	public final String[] relationTableNames;

	public final String[] relationCacheNames;

	private IRStyleExperiment(String[] tableNames, String[][] textAttribs, String popularity, String[] cacheNames,
			int[] limits, int[] sizes, String dataDir, DatabaseType databaseType, String[] relationTableNames,
			String[] relationCacheNames) {
		this.tableNames = tableNames;
		this.textAttribs = textAttribs;
		this.popularity = popularity;
		this.cacheNames = cacheNames;
		this.limits = limits;
		this.sizes = sizes;
		this.dataDir = dataDir;
		this.databaseType = databaseType;
		this.relationTableNames = relationTableNames;
		this.relationCacheNames = relationCacheNames;
	}

	public static IRStyleExperiment createWikiMsnExperiment() {
		String[] cacheName = new String[Wiki_Constants.tableName.length];
		for (int i = 0; i < Wiki_Constants.tableName.length; i++) {
			cacheName[i] = "sub_" + Wiki_Constants.tableName[i].substring(4) + "_mrr";
		}
		return new IRStyleExperiment(Wiki_Constants.tableName, Wiki_Constants.textAttribs, "popularity", cacheName,
				Wiki_Constants.mrrLimit, Wiki_Constants.size, Wiki_Constants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA,
				Wiki_Constants.relationTable, Wiki_Constants.relationCacheTable);
	}

	public static IRStyleExperiment createWikiP20Experiment() {
		String[] cacheName = new String[Wiki_Constants.tableName.length];
		for (int i = 0; i < Wiki_Constants.tableName.length; i++) {
			cacheName[i] = "sub_" + Wiki_Constants.tableName[i].substring(4) + "_p20";
		}
		return new IRStyleExperiment(Wiki_Constants.tableName, Wiki_Constants.textAttribs, "popularity", cacheName,
				Wiki_Constants.precisionLimit, Wiki_Constants.size, Wiki_Constants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA,
				Wiki_Constants.relationTable, Wiki_Constants.relationCacheTable);
	}

	public static IRStyleExperiment createWikiRecExperiment() {
		String[] cacheName = new String[Wiki_Constants.tableName.length];
		for (int i = 0; i < Wiki_Constants.tableName.length; i++) {
			cacheName[i] = "sub_" + Wiki_Constants.tableName[i].substring(4) + "_rec";
		}
		return new IRStyleExperiment(Wiki_Constants.tableName, Wiki_Constants.textAttribs, "popularity", cacheName,
				Wiki_Constants.recallLimit, Wiki_Constants.size, Wiki_Constants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA,
				Wiki_Constants.relationTable, Wiki_Constants.relationCacheTable);
	}

	public static IRStyleExperiment createStackExperiment() {
		String[] cacheName = new String[Stack_Constants.tableName.length];
		for (int i = 0; i < Stack_Constants.tableName.length; i++) {
			cacheName[i] = "sub_" + Stack_Constants.tableName[i] + "_mrr";
		}
		return new IRStyleExperiment(Stack_Constants.tableName, Stack_Constants.textAttribs, "ViewCount", cacheName,
				Stack_Constants.cacheSize, Stack_Constants.size, Stack_Constants.DATA_STACK, DatabaseType.STACKOVERFLOW,
				Stack_Constants.relationTables, Stack_Constants.relationCacheTables);
	}

}
