package amazon;

import java.io.File;

public class AmazonDirectoryInfo {

	// private static final String HOME = "/scratch/cluster-share/ghadakcv/";
	private static final String HOME = "/scratch/";
	static final String DATA_SET_DIR = HOME + "data-sets/amazon/amazon-inex/";
	static final String FILE_LIST = HOME + "data/path_counts/amazon_path_reviews.csv";
	static final String GLOBAL_INDEX_DIR = HOME + "data/index/amazon/";
	static final String QUERY_FILE = HOME + "data/queries/amazon/inex2014sbs.topics.xml";
	static final String QREL_FILE = HOME + "data/queries/amazon/inex14sbs.qrels";
	static final String RESULT_DIR = HOME + "data/result/amazon/";
	static final String ISBN_DICT = HOME + "data/queries/amazon/amazon-lt.isbn.thingID.csv";
	static final String DEWEY_DICT = HOME + "data/dewey.csv";
	
	static {
		File resultDir = new File(RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();
	}

}
