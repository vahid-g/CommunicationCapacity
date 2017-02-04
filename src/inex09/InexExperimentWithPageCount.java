package inex09;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import freebase.FreebaseDatabaseSizeExperiment;

public class InexExperimentWithPageCount {

	static final Logger LOGGER = Logger.getLogger(FreebaseDatabaseSizeExperiment.class.getName());

	public static void main(String[] args) {
		// initializations
		LOGGER.setUseParentHandlers(false);
		Handler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		LOGGER.addHandler(handler);
		LOGGER.setLevel(Level.ALL);

		File indexBaseDir = new File(ClusterDirectoryInfo.LOCAL_INDEX_BASE);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(ClusterDirectoryInfo.RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();

		int expNo = Integer.parseInt(args[0]);
		exp(expNo);
	}

	public static void exp(int expNo) {
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE + "index_inex_" + expNo;
		try {
			LOGGER.log(Level.INFO, "Loading files path and counts");
			Map<String, Integer> pathCountMap = new HashMap<String, Integer>();
			try (BufferedReader br = new BufferedReader(new FileReader(ClusterDirectoryInfo.PATH_COUNT_FILE))) {
				String line;
				while ((line = br.readLine()) != null) {
					if (!line.contains(","))
						continue;
					String path = ClusterDirectoryInfo.CLUSTER_BASE + line.split(",")[0];
					Integer count = Integer.parseInt(line.split(",")[1].trim());
					if (pathCountMap.containsKey(path))
						pathCountMap.put(path, count + pathCountMap.get(path));
					else
						pathCountMap.put(path, count);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountMap.size());
			LOGGER.log(Level.INFO, "Sorting files..");
			int subsetSize = (int) (pathCountMap.size() * (expNo / 10.0));
			Map<String, Integer> pathCountSorted = Utils.sortByValue(pathCountMap, subsetSize);
			LOGGER.log(Level.INFO, "Building index..");
			InexIndexer.buildIndex(pathCountSorted, indexName);

			LOGGER.log(Level.INFO, "Loading and running queries..");
			List<InexQuery> queries = InexQueryServices.loadInexQueries(ClusterDirectoryInfo.INEX9_QUERY_FILE);
			LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
			List<InexQueryResult> iqrList = InexQueryServices.runInexQueries(queries, indexName);
			LOGGER.log(Level.INFO, "Writing results..");
			try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR + "inex_" + expNo + ".csv")) {
				for (InexQueryResult iqr : iqrList) {
					fw.write(iqr.toString());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			LOGGER.log(Level.INFO, "cleanup..");
			try {
				File indexFile = new File(indexName);
				if (indexFile.exists())
					FileUtils.deleteDirectory(indexFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

}
