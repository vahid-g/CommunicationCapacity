package inex13;

import inex09.ClusterDirectoryInfo;
import inex09.InexQueryServices;
import inex09.MsnQuery;
import inex09.MsnQueryResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class ClusterMsnExperiment {

	static final Logger LOGGER = Logger.getLogger(ClusterMsnExperiment.class.getName());

	static class PathCountTitle {
		String path;
		Integer visitCount;
		String title;

		public PathCountTitle(String path, Integer visitCount) {
			this.path = path;
			this.visitCount = visitCount;
		}
		
		public PathCountTitle(String path, Integer visitCount, String title) {
			this.path = path;
			this.visitCount = visitCount;
			this.title = title;
		}
	}

	public static void main(String[] args) {
		File indexBaseDir = new File(ClusterDirectoryInfo.LOCAL_INDEX_BASE13);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(ClusterDirectoryInfo.RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();

		// float gamma = Float.parseFloat(args[0]);
		// gridSearchExperiment(gamma);

		int expNo = Integer.parseInt(args[0]);
		long start_t = System.currentTimeMillis();
		exp(expNo);
		long end_t = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "Time spent for experiment " + expNo + " is " + (end_t - start_t) / 60000 + " minutes");
	}

	static void gridSearchExperiment(float gamma) {
		List<PathCountTitle> pathCountList = loadFilePathPageVisit();
		// TODO sort?
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "inex13_grid_" + (gamma * 10);
		LOGGER.log(Level.INFO, "Building index..");
		InexIndexer.buildIndex(pathCountList, indexName, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID_S,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR + "inex13_grid_" + (gamma * 10) + ".csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write(mqr.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void exp(int expNo) {
		LOGGER.log(Level.INFO, "Loading files list and counts");
		List<PathCountTitle> pathCountList = loadFilePathPageVisit();
		pathCountList = pathCountList.subList(0, (int) ((expNo / 10.0) * pathCountList.size()));
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
		LOGGER.log(Level.INFO, "Building index..");
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "inex13_" + expNo;
		InexIndexer.buildIndex(pathCountList, indexName, 0); // TODO set gamma

		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID_B,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR + "inex_" + expNo + ".csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write(mqr.toString());
			}
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void expText(int expNo) {
		LOGGER.log(Level.INFO, "Loading files list and counts");
		List<PathCountTitle> pathCountList = loadFilePathPageVisit();
		pathCountList = pathCountList.subList(0, (int) ((expNo / 10.0) * pathCountList.size()));
		LOGGER.log(Level.INFO, "Number of loaded path_counts: " + pathCountList.size());
		LOGGER.log(Level.INFO, "Building index..");
		String indexName = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "inex13_" + expNo;
		InexIndexer.buildIndexOnText(pathCountList, indexName, 0); // TODO set gamma

		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(ClusterDirectoryInfo.MSN_QUERY_QID_B,
				ClusterDirectoryInfo.MSN_QID_QREL);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries, indexName);
		LOGGER.log(Level.INFO, "Writing results..");
		try (FileWriter fw = new FileWriter(ClusterDirectoryInfo.RESULT_DIR + "inex_" + expNo + ".csv")) {
			for (MsnQueryResult mqr : results) {
				fw.write(mqr.toString());
			}
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static List<PathCountTitle> loadFilePathPageVisit() {
		LOGGER.log(Level.INFO, "Loading files list and sorted counts..");
		List<PathCountTitle> pathCountList = new ArrayList<PathCountTitle>();
		try (BufferedReader br = new BufferedReader(new FileReader(ClusterDirectoryInfo.PATH_COUNT_FILE13))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (!line.contains(","))
					continue;
				String[] fields = line.split(", ");
				String path = ClusterDirectoryInfo.CLUSTER_BASE + fields[0];
				Integer count = Integer.parseInt(fields[1].trim());
				String title = fields[3].trim();
				pathCountList.add(new PathCountTitle(path, count, title));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return pathCountList;
	}

	
	 
	// how to load inex13 queries:
	//
	// String QUERY_FILE = ClusterDirectoryInfo.CLUSTER_BASE +
	// "data/queries/inex_ld/2013-ld-adhoc-topics.xml"; String QREL_FILE =
	// ClusterDirectoryInfo.CLUSTER_BASE +
	// "data/queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
	// HashMap<Integer, InexQueryDAO> queriesMap =
	// QueryParser.buildQueries(QUERY_FILE, QREL_FILE); List<InexQueryDAO>
	// queries = new ArrayList<InexQueryDAO>();
	// queries.addAll(queriesMap.values()); List<InexQueryResult> resultList =
	// QueryServices.runQueries(queries, indexPath);
	 

}
