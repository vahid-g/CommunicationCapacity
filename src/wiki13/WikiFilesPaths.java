package wiki13;

public abstract class WikiFilesPaths {
    public String INDEX_BASE;
    public String FILELIST_PATH;
    public String FILELIST_PATH_COUNT09;
    public String QUERYFILE_PATH;
    public String QREL_PATH;
    public String MSN_QUERY_QID;
    public String MSN_QID_QREL;

    public static class ClusterPaths extends WikiFilesPaths {
	public ClusterPaths() {
	    INDEX_BASE = "/scratch/cluster-share/ghadakcv/data/index/";
	    FILELIST_PATH = "/scratch/cluster-share/ghadakcv/data/path_counts/wiki13_count13_text.csv";
	    FILELIST_PATH_COUNT09 = "/scratch/cluster-share/ghadakcv/data/path_counts/wiki13_count09_text.csv";
	    QUERYFILE_PATH = "/scratch/cluster-share/ghadakcv/data/queries/inex_ld/2013-ld-adhoc-topics.xml";
	    QREL_PATH = "/scratch/cluster-share/ghadakcv/data/queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
	    MSN_QUERY_QID = "/scratch/cluster-share/ghadakcv/data/queries/msn/query_qid.csv";
	    MSN_QID_QREL = "/scratch/cluster-share/ghadakcv/data/queries/msn/qid_qrel.csv";
	}
    }

    public static class MaplePaths extends WikiFilesPaths {
	public MaplePaths() {
	    String DATA_PATH = "/data/ghadakcv/";
	    INDEX_BASE = DATA_PATH + "wiki_index/";
	    FILELIST_PATH = DATA_PATH + "wiki13_count13_text.csv";
	    FILELIST_PATH_COUNT09 = DATA_PATH + "wiki13_count09_text.csv";
	    QUERYFILE_PATH = "/nfs/stak/users/ghadakcv/workspace/queries/2013-adhoc.xml";
	    QREL_PATH = "/nfs/stak/users/ghadakcv/workspace/queries/2013-adhoc.qrels";
	    MSN_QUERY_QID = "/nfs/stak/users/ghadakcv/workspace/queries/msn_query_qid.csv";
	    MSN_QID_QREL = "/nfs/stak/users/ghadakcv/workspace/queries/msn.qrels";
	}
    }

}
