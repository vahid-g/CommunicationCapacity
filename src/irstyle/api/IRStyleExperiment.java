package irstyle.api;

import database.DatabaseType;
import irstyle.WikiConstants;
import stackoverflow.irstyle.StackConstants;

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
		String[] cacheName = new String[WikiConstants.tableName.length];
		for (int i = 0; i < WikiConstants.tableName.length; i++) {
			cacheName[i] = "sub_" + WikiConstants.tableName[i].substring(4) + "_mrr";
		}
		return new IRStyleExperiment(WikiConstants.tableName, WikiConstants.textAttribs, "popularity", cacheName,
				WikiConstants.mrrLimit, WikiConstants.size, WikiConstants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA,
				WikiConstants.relationTable, WikiConstants.relationCacheTable);
	}

	public static IRStyleExperiment createWikiP20Experiment() {
		String[] cacheName = new String[WikiConstants.tableName.length];
		for (int i = 0; i < WikiConstants.tableName.length; i++) {
			cacheName[i] = "sub_" + WikiConstants.tableName[i].substring(4) + "_p20";
		}
		return new IRStyleExperiment(WikiConstants.tableName, WikiConstants.textAttribs, "popularity", cacheName,
				WikiConstants.precisionLimit, WikiConstants.size, WikiConstants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA,
				WikiConstants.relationTable, WikiConstants.relationCacheTable);
	}

	public static IRStyleExperiment createWikiRecExperiment() {
		String[] cacheName = new String[WikiConstants.tableName.length];
		for (int i = 0; i < WikiConstants.tableName.length; i++) {
			cacheName[i] = "sub_" + WikiConstants.tableName[i].substring(4) + "_rec";
		}
		return new IRStyleExperiment(WikiConstants.tableName, WikiConstants.textAttribs, "popularity", cacheName,
				WikiConstants.recallLimit, WikiConstants.size, WikiConstants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA,
				WikiConstants.relationTable, WikiConstants.relationCacheTable);
	}

	public static IRStyleExperiment createStackExperiment() {
		String[] cacheName = new String[StackConstants.tableName.length];
		for (int i = 0; i < StackConstants.tableName.length; i++) {
			cacheName[i] = "sub_" + StackConstants.tableName[i] + "_mrr";
		}
		return new IRStyleExperiment(StackConstants.tableName, StackConstants.textAttribs, "ViewCount", cacheName,
				StackConstants.cacheSize, StackConstants.size, StackConstants.DATA_STACK, DatabaseType.STACKOVERFLOW,
				StackConstants.relationTables, StackConstants.relationCacheTables);
	}

}
