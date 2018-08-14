package wiki13;

public class WikiFilesPaths {

	public static WikiFilesPaths getHpcPaths() {
		if (hpcPaths == null) {
			String dataPath = "/scratch/cluster-share/ghadakcv/data/";
			String indexBase = dataPath + "index/wiki13_p50_w13/";
			String accessCountsPath = dataPath + "path_counts/wiki13_count13_text.csv";
			String accessCounts09Path = dataPath + "path_counts/wiki13_count09_text.csv";
			String biwordIndexPath = dataPath + "index/wiki13_p50_w13_bi";
			hpcPaths = new WikiFilesPaths(dataPath, indexBase, accessCountsPath, accessCounts09Path, biwordIndexPath);
		}
		return hpcPaths;
	}

	public static WikiFilesPaths getMaplePaths() {
		if (maplePaths == null) {
			String dataPath = "/data/ghadakcv/";
			String indexBase = dataPath + "wiki_index/";
			String accessCountsPath = dataPath + "wiki13_count13_text.csv";
			String accessCounts09Path = dataPath + "wiki13_count09_text.csv";
			String biwordIndexBase = dataPath + "wiki_index_bi/";
			maplePaths = new WikiFilesPaths(dataPath, indexBase, accessCountsPath, accessCounts09Path, biwordIndexBase);
		}
		return maplePaths;
	}

	private static WikiFilesPaths maplePaths = null;

	private static WikiFilesPaths hpcPaths = null;

	public String getDataFolder() {
		return dataFolder;
	}

	public String getIndexBase() {
		return indexBase;
	}

	public String getAccessCountsPath() {
		return accessCountsPath;
	}

	public String getAccessCounts09Path() {
		return accessCounts09Path;
	}

	public String getInexQueryFilePath() {
		return inexQueryFilePath;
	}

	public String getInexQrelFilePath() {
		return inexQrelFilePath;
	}

	public String getMsnQueryFilePath() {
		return msnQueryFilePath;
	}

	public String getMsnQrelFilePath() {
		return msnQrelFilePath;
	}

	public String getMsnAllFilePath() {
		return msnAllFilePath;
	}

	public String getBiwordIndexBase() {
		return biwordIndexBase;
	}

	private final String dataFolder;
	private final String indexBase;
	private final String accessCountsPath;
	private final String accessCounts09Path;
	private final String inexQueryFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/2013-adhoc.xml";
	private final String inexQrelFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/2013-adhoc.qrels";
	private final String msnQueryFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/msn_query_qid.csv";
	private final String msnQrelFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/msn.qrels";
	private final String msnAllFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/msn_all.csv";
	private final String biwordIndexBase;

	private WikiFilesPaths(String dataFolder, String indexBase, String accessCountsPath, String accessCounts09Path,
			String biwordIndexBase) {
		this.dataFolder = dataFolder;
		this.indexBase = indexBase;
		this.accessCountsPath = accessCountsPath;
		this.accessCounts09Path = accessCounts09Path;
		this.biwordIndexBase = biwordIndexBase;
	}

}
