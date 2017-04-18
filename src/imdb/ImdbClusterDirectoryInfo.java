package imdb;

import java.io.File;

import wiki_inex09.ClusterDirectoryInfo;

public class ImdbClusterDirectoryInfo {
	
	static final String HOME = "/scratch/cluster-share/ghadakcv/";
	static final String DATA_SET = HOME + "beautified-imdb/movies/";
	static final String LOCAL_INDEX = "/scratch/ghadakcv/index";
	static final String RESULT_DIR = HOME + "data/result/";
	
	static {
		File indexBaseDir = new File(ClusterDirectoryInfo.LOCAL_INDEX_BASE13);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(ClusterDirectoryInfo.RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();
	}
	

}
