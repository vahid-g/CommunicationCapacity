package inex13;

import freebase.FreebaseDatabaseSizeExperiment;
import inex09.ClusterDirectoryInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class ClusterExperiment {

	final static String FILE_LIST = ClusterDirectoryInfo.CLUSTER_BASE
			+ "data/filelist.txt";
	final static String QUERY_FILE = ClusterDirectoryInfo.CLUSTER_BASE
			+ "data/queries/inex_ld/2013-ld-adhoc-topics.xml";
	final static String QREL_FILE = ClusterDirectoryInfo.CLUSTER_BASE
			+ "data/queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
	final static String RESULT_DIR = ClusterDirectoryInfo.CLUSTER_BASE
			+ "data/result/inex13_dbsize/";

	static final Logger LOGGER = Logger
			.getLogger(FreebaseDatabaseSizeExperiment.class.getName());

	public static void main(String[] args) {
		// initializations
		LOGGER.setUseParentHandlers(false);
		Handler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		LOGGER.addHandler(handler);
		LOGGER.setLevel(Level.ALL);
		File indexBaseDir = new File(ClusterDirectoryInfo.LOCAL_INDEX_BASE09);
		if (!indexBaseDir.exists())
			indexBaseDir.mkdirs();
		File resultDir = new File(ClusterDirectoryInfo.RESULT_DIR);
		if (!resultDir.exists())
			resultDir.mkdirs();

		double percent = Double.parseDouble(args[0]);

	}

	// requires a sorted file list based on their pagecount!
	static void createGlobalIndex(double percent) {
		LOGGER.log(Level.INFO, "Loading files list and counts");
		List<Map.Entry<String, Integer>> pathCountList = new ArrayList<Map.Entry<String, Integer>>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				ClusterDirectoryInfo.PATH_COUNT_FILE13))) {
			String line;
			while ((line = br.readLine()) != null) {
				if (!line.contains(","))
					continue;
				String path = ClusterDirectoryInfo.CLUSTER_BASE + "inex_13/"
						+ line.split(",")[0];
				Integer count = Integer.parseInt(line.split(",")[1].trim());
				/*if (pathCountMap.containsKey(path))
					pathCountMap.put(path, count + pathCountMap.get(path));
				else
					pathCountMap.put(path, count);*/
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void oldExperiment() {
		int expNo = Integer.parseInt("???");
		String indexPath = ClusterDirectoryInfo.LOCAL_INDEX_BASE13 + "exp_"
				+ expNo;
		randomizedDbSizeSinglePartition(expNo, indexPath);
		File indexFile = new File(indexPath);
		// cleanup
		try {
			FileUtils.deleteDirectory(indexFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * builds index for a single partition and runs the queries on it. To be
	 * able to run this method you need to have a list of all dataset files.
	 * Then based on the expNo parameter the method will pick a subset of the
	 * files and run the code.
	 * 
	 * @param expNo
	 *            number of experiment that will also impact the partition size
	 */
	static void randomizedDbSizeSinglePartition(int expNo, String indexPath) {
		DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
		System.out.println(df.format(new Date()) + " running experiment "
				+ expNo);
		List<String> fileList = null;
		try {
			fileList = Files.readAllLines(Paths.get(FILE_LIST),
					StandardCharsets.UTF_8);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		List<String> expFileList = fileList.subList(0,
				(int) ((expNo / 10.0) * fileList.size()));
		System.out.println(df.format(new Date()) + " building index..");
		inex09.InexIndexer.buildIndex(expFileList.toArray(new String[0]),
				indexPath, false);

		// running queries
		System.out.println(df.format(new Date()) + " running queries..");
		HashMap<Integer, InexQueryDAO> queriesMap = QueryParser.buildQueries(
				QUERY_FILE, QREL_FILE);
		List<InexQueryDAO> queries = new ArrayList<InexQueryDAO>();
		queries.addAll(queriesMap.values());
		double p20Map[] = new double[queries.size()];
		double p3Map[] = new double[queries.size()];
		List<InexQueryResult> resultList = QueryServices.runQueries(queries,
				indexPath);
		for (int j = 0; j < resultList.size(); j++) {
			p20Map[j] = resultList.get(j).precisionAtK(20);
			p3Map[j] = resultList.get(j).precisionAtK(3);
		}
		// writing results to file
		System.out.println(df.format(new Date()) + " writing output to file..");
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + "inex_" + expNo + ".csv");
			for (int i = 0; i < p20Map.length; i++) {
				fw.write("\"" + queries.get(i).text + "\"" + ", ");
				fw.write(p20Map[i] + ", " + p3Map[i] + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
