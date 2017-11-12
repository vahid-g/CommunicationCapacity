package wiki13;

import java.io.File;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.search.similarities.BM25Similarity;

import indexing.InexFile;

public class WikiMapleExperiment {
	
	private static final Logger LOGGER = Logger.getLogger(WikiMapleExperiment.class.getName());
	private static final String DATA_PATH = "/data/ghadakcv/data/";
	private static final String INDEX_PATH = DATA_PATH + "wiki_index";
	private static final String FILELIST_PATH = DATA_PATH + "/wiki13_count09_text.csv";
	
	public static void main(String[] args) {
		buildIndex(FILELIST_PATH, INDEX_PATH);
	}
	
	private static void buildIndex(String fileListPath, String indexDirectoryPath) {
		try {
			List<InexFile> pathCountList = InexFile.loadInexFileList(fileListPath);
			LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
			File indexPathFile = new File(indexDirectoryPath);
			if (!indexPathFile.exists()) {
				indexPathFile.mkdirs();
			}
			LOGGER.log(Level.INFO, "Building index at: " + indexDirectoryPath);
			Wiki13Indexer.buildIndexOnText(pathCountList, indexDirectoryPath, new BM25Similarity());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
