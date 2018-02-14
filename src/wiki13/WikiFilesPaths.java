package wiki13;

public class WikiFilesPaths {

	public static WikiFilesPaths getHpcPaths() {
		if (hpcPaths == null) {
			String dataPath = "/scratch/cluster-share/ghadakcv/data/";
			String indexBase = dataPath + "index/";
			String accessCountsPath = dataPath + "path_counts/wiki13_count13_text.csv";
			String accessCounts09Path = dataPath + "path_counts/wiki13_count09_text.csv";
			hpcPaths = new WikiFilesPaths(indexBase, accessCountsPath, accessCounts09Path);
		}
		return hpcPaths;
	}

	public static WikiFilesPaths getMaplePaths() {
		if (maplePaths == null) {
			String dataPath = "/data/ghadakcv/";
			String indexBase = dataPath + "wiki_index/";
			String accessCountsPath = dataPath + "wiki13_count13_text.csv";
			String accessCounts09Path = dataPath + "wiki13_count09_text.csv";
			maplePaths = new WikiFilesPaths(indexBase, accessCountsPath, accessCounts09Path);
		}
		return maplePaths;
	}

	private static WikiFilesPaths maplePaths = null;

	private static WikiFilesPaths hpcPaths = null;

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

	private final String indexBase;
	private final String accessCountsPath;
	private final String accessCounts09Path;
	private final String inexQueryFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/2013-adhoc.xml";
	private final String inexQrelFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/2013-adhoc.qrels";
	private final String msnQueryFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/msn_query_qid.csv";
	private final String msnQrelFilePath = "/nfs/stak/users/ghadakcv/workspace/queries/msn.qrels";

	private WikiFilesPaths(String indexBase, String accessCountsPath, String accessCounts09Path) {
		this.indexBase = indexBase;
		this.accessCountsPath = accessCountsPath;
		this.accessCounts09Path = accessCounts09Path;
	}

}
