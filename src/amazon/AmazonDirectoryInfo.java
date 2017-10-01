package amazon;

import java.io.File;

public class AmazonDirectoryInfo {

	public static final String HOME = "/scratch/cluster-share/ghadakcv/";
	//public static final String HOME = "/scratch/";
	public static final String DATA_SET_DIR = HOME + "data-sets/amazon/amazon-inex/";
	public static final String FILE_LIST = HOME + "data/path_counts/amazon_path_reviews.csv";
	public static final String GLOBAL_INDEX_DIR = HOME + "data/index/amazon/";
	public static final String QUERY_FILE = HOME + "data/queries/amazon/inex2014sbs.topics.xml";
	public static final String TEST_QUERY_FILE = HOME + "data/queries/amazon/test_topics.xml";
	public static final String QREL_FILE = HOME + "data/queries/amazon/inex14sbs.qrels";
	public static final String RESULT_DIR = HOME + "data/result/amazon/";
	public static final String ISBN_DICT = HOME + "data/queries/amazon/amazon-lt.isbn.thingID.csv";
	public static final String DEWEY_DICT = HOME + "data/dewey.csv";
	
	static {
		File resultDir = new File(RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();
	}

}
