package wiki_inex09;

import java.io.File;

public class ClusterDirectoryInfo {

	public static final String CLUSTER_BASE = "/scratch/cluster-share/ghadakcv/";
	public static final String MSN_QUERY_QID = CLUSTER_BASE
			+ "data/msn/query_qid.csv";
	public static final String MSN_QID_QREL = CLUSTER_BASE
			+ "data/msn/qid_qrel.csv";
	public static final String MSN_QUERY_QID_S = CLUSTER_BASE
			+ "data/msn/query_qid_small.csv";
	public static final String MSN_QUERY_QID_B = CLUSTER_BASE
			+ "data/msn/query_qid_big.csv";
	public static final String GLOBAL_INDEX_BASE = "/scratch/cluster-share/ghadakcv/data/index/";
	public static final String RESULT_DIR = CLUSTER_BASE + "data/result/";

	// 2009 files
	public static final String DATASET09_PATH = CLUSTER_BASE
			+ "data-sets/inex_09";
	public static final String LOCAL_INDEX_BASE09 = "/scratch/ghadakcv/index09/";
	public static final String INEX9_QUERY_FILE = CLUSTER_BASE
			+ "data/inex9_queries/queries.csv";
	public static final String INEX9_QREL_FILE = CLUSTER_BASE
			+ "data/inex9_queries/queries.qrel";
	public static final String INEX10_QUERY_FILE = CLUSTER_BASE
			+ "data/inex10_queries/queries.csv";
	public static final String PATH_COUNT_FILE09 = CLUSTER_BASE
			+ "data/path_counts/pathcount_09_0103.csv";

	// 2013 files
	public static final String DATASET13_PATH = CLUSTER_BASE
			+ "data-sets/inex_13";
	public static final String LOCAL_INDEX_BASE13 = "/scratch/ghadakcv/index13/";
	public static final String PATH13_COUNT13 = CLUSTER_BASE
			+ "data/path_counts/pathcount_13_0103_text.csv";
	public static final String PATH13_COUNT09 = CLUSTER_BASE
			+ "data/path_counts/path13_count09_text.csv";

	// INEX 2013 query files
	public static final String INEX13_QUERY_FILE = ClusterDirectoryInfo.CLUSTER_BASE
			+ "data/inex_ld/2013-ld-adhoc-topics.xml";
	public static final String INEX13_QREL_FILE = ClusterDirectoryInfo.CLUSTER_BASE
			+ "data/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";

	static {
		File indexBaseDir = new File(ClusterDirectoryInfo.LOCAL_INDEX_BASE13);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(ClusterDirectoryInfo.RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();
	}

}
