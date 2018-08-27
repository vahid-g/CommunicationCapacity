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

	public final String dataDir;

	public final DatabaseType databaseType;

	private IRStyleExperiment(String[] tableNames, String[][] textAttribs, String popularity, String[] cacheNames,
			int[] limits, String dataDir, DatabaseType databaseType) {
		this.tableNames = tableNames;
		this.textAttribs = textAttribs;
		this.popularity = popularity;
		this.cacheNames = cacheNames;
		this.limits = limits;
		this.dataDir = dataDir;
		this.databaseType = databaseType;
	}

	public static IRStyleExperiment createWikiMrrExperiment() {
		String[] cacheName = new String[WikiConstants.tableName.length];
		for (int i = 0; i < WikiConstants.tableName.length; i++) {
			cacheName[i] = "sub_" + WikiConstants.tableName[i].substring(4) + "_mrr";
		}
		return new IRStyleExperiment(WikiConstants.tableName, WikiConstants.textAttribs, "popularity", cacheName,
				WikiConstants.mrrLimit, WikiConstants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA);
	}

	public static IRStyleExperiment createWikiP20Experiment() {
		String[] cacheName = new String[WikiConstants.tableName.length];
		for (int i = 0; i < WikiConstants.tableName.length; i++) {
			cacheName[i] = "sub_" + WikiConstants.tableName[i].substring(4) + "_p20";
		}
		return new IRStyleExperiment(WikiConstants.tableName, WikiConstants.textAttribs, "popularity", cacheName,
				WikiConstants.precisionLimit, WikiConstants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA);
	}

	public static IRStyleExperiment createWikiRecExperiment() {
		String[] cacheName = new String[WikiConstants.tableName.length];
		for (int i = 0; i < WikiConstants.tableName.length; i++) {
			cacheName[i] = "sub_" + WikiConstants.tableName[i].substring(4) + "_rec";
		}
		return new IRStyleExperiment(WikiConstants.tableName, WikiConstants.textAttribs, "popularity", cacheName,
				WikiConstants.recallLimit, WikiConstants.WIKI_DATA_DIR, DatabaseType.WIKIPEDIA);
	}

	public static IRStyleExperiment createStackExperiment() {
		String[] cacheName = new String[WikiConstants.tableName.length];
		for (int i = 0; i < WikiConstants.tableName.length; i++) {
			cacheName[i] = "sub_" + WikiConstants.tableName[i] + "_mrr";
		}
		return new IRStyleExperiment(StackConstants.tableName, StackConstants.textAttribs, "TrainViewCount", cacheName,
				StackConstants.cacheSize, StackConstants.DATA_STACK, DatabaseType.STACKOVERFLOW);
	}

}
