package inex;

import inex_msn.InexMsnIndexer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class ClusterExperiment {

	final static String INDEX_PATH = "/scratch/ghadakcv/index/";

	final static String DATA_FOLDER = "/scratch/cluster_share/ghadakcv/";
	final static String DATA_SET = DATA_FOLDER + "inex_13/";
	final static String FILE_LIST = DATA_FOLDER + "filelist.txt";
	final static String QUERY_FILE = DATA_FOLDER
			+ "queries/inex_ld/2013-ld-adhoc-topics.xml";
	final static String QREL_FILE = DATA_FOLDER
			+ "queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
	final static String RESULT_DIR = DATA_FOLDER + "result/inex13_dbsize/";

	public static void main(String[] args) {
		randomizedDbSizeSinglePartition(Integer.parseInt(args[0]));

		// cleanup
		File indexFile = new File(INDEX_PATH);
		indexFile.delete();
	}

	// builds index for a single partition and runs the queries on it. To be
	// able to run
	// this method you need to have a list of all dataset files. Then based on
	// the expNo parameter
	// the method will pick a subset of the files and run the code.
	static void randomizedDbSizeSinglePartition(int expNo) {
		System.out.println(new Date().toString() + " listing files..");
		List<String> fileList = null;
		try {
			fileList = Files.readAllLines(Paths.get(FILE_LIST),
					StandardCharsets.UTF_8);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		List<String> expFileList = fileList.subList(0,
				(int) ((expNo / 10.0) * fileList.size()));
		List<String> procFileList = new ArrayList<String>();
		for (String filename : expFileList) {
			procFileList.add(DATA_SET + filename);
		}
		String indexPath = INDEX_PATH + "exp_" + expNo;
		InexMsnIndexer.buildIndex(procFileList.toArray(new String[0]), indexPath,
				false);

		// running queries
		HashMap<Integer, InexQueryDAO> queriesMap = QueryParser.buildQueries(
				QUERY_FILE, QREL_FILE);
		List<InexQueryDAO> queries = new ArrayList<InexQueryDAO>();
		queries.addAll(queriesMap.values());
		System.out.println(new Date().toString() + " submitting queries");
		double preMap[] = new double[queries.size()];
		List<InexQueryResult> resultList = QueryServices.runQueries(queries,
				indexPath);
		for (int j = 0; j < resultList.size(); j++) {
			preMap[j] = resultList.get(j).precisionAtK(20);
		}
		// writing results to file
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + "inex_" + expNo + ".csv");
			for (int i = 0; i < preMap.length; i++) {
				fw.write("\"" + queries.get(i).text + "\"" + ", ");
				fw.write(preMap[i] + "\n");
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
