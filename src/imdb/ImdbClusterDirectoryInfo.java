package imdb;

import java.io.File;

public class ImdbClusterDirectoryInfo {

    static final String HOME = "/scratch/cluster-share/ghadakcv/";
    static final String DATA_SET = HOME + "data-sets/imdb-inex/movies/";
    static final String FILE_LIST = HOME
	    + "data/path_counts/imdb_path_ratings.csv";
    static final String LOCAL_INDEX = "/scratch/ghadakcv/index/";
    static final String QUERY_FILE = HOME + "data/queries/imdb.xml";
    static final String QREL_FILE = HOME + "data/queries/imdb.qrels";
    static final String RESULT_DIR = HOME + "data/result/imdb/";

    static {
	File indexBaseDir = new File(LOCAL_INDEX);
	if (!indexBaseDir.exists())
	    indexBaseDir.mkdirs();
	File resultDir = new File(RESULT_DIR);
	if (!resultDir.exists())
	    resultDir.mkdirs();
    }

}
