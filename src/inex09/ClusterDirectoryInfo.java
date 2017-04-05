package inex09;

import java.io.File;

public class ClusterDirectoryInfo {

	public static final String CLUSTER_BASE = "/scratch/cluster-share/ghadakcv/";
	public static final String MSN_QUERY_QID = CLUSTER_BASE + "data/msn/query_qid.csv";
	public static final String MSN_QID_QREL = CLUSTER_BASE + "data/msn/qid_qrel.csv";
	public static final String MSN_QUERY_QID_S = CLUSTER_BASE + "data/msn/query_qid_small.csv";
	public static final String MSN_QUERY_QID_B = CLUSTER_BASE + "data/msn/query_qid_big.csv";
	
	public static final String RESULT_DIR = CLUSTER_BASE + "data/result/";
	
	// 2009 files
	public static final String DATASET09_PATH = CLUSTER_BASE + "inex_09";
	public static final String LOCAL_INDEX_BASE09 = "/scratch/ghadakcv/index09/";
	public static final String GLOBAL_INDEX_BASE09 = "/scratch/cluster-share/ghadakcv/index09/";
	public static final String INEX9_QUERY_FILE = CLUSTER_BASE
			+ "data/inex9_queries/queries.csv";
	public static final String INEX10_QUERY_FILE = CLUSTER_BASE
			+ "data/inex10_queries/queries.csv";
	public static final String PATH_COUNT_FILE09 = CLUSTER_BASE + "data/path_counts/pathcount_09_0103.csv";
	
	//2013 files
	public static final String DATASET13_PATH = CLUSTER_BASE + "inex_13";
	public static final String LOCAL_INDEX_BASE13 = "/scratch/ghadakcv/index13/";
	public static final String GLOBAL_INDEX_BASE13 = "/scratch/cluster-share/ghadakcv/index13/";
	public static final String PATH_COUNT_FILE13 = CLUSTER_BASE + "data/path_counts/pathcount_13_0103_text.csv";
	
	static {
		File indexBaseDir = new File(ClusterDirectoryInfo.LOCAL_INDEX_BASE13);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(ClusterDirectoryInfo.RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();
	}
	
	

	
}
