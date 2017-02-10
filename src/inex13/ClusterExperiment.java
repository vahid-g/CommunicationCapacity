package inex13;

import java.io.File;
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

import org.apache.commons.io.FileUtils;

public class ClusterExperiment {


	public static void main(String[] args) {
		
	}
	
	static void runOldExp(){
		int expNo = Integer.parseInt("???");
		String indexPath = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "exp_" + expNo;
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
