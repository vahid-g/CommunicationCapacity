package amazon;

import java.io.File;

public class AmazonDirectoryInfo {

	static final String HOME = "/scratch/cluster-share/ghadakcv/";
	static final String DATA_SET = HOME + "data-sets/amazon/amazon-inex/";
	static final String FILE_LIST = HOME + "data/path_counts/amazon_path_reviews.csv";
	static final String LOCAL_INDEX = "/scratch/ghadakcv/index/";
	static final String GLOBAL_INDEX = HOME + "data/index/";
	static final String QUERY_FILE = HOME + "data/queries/amazon/inex2014sbs.topics.xml";
	static final String QREL_FILE = HOME + "data/queries/amazon/inex14sbs.qrels";
	static final String RESULT_DIR = HOME + "data/result/amazon/";

	static final String ISBN_DICT = HOME + "data/queries/amazon/amazon-lt.isbn.thingID.csv";

	static {
		File indexBaseDir = new File(LOCAL_INDEX);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();
	}

}
