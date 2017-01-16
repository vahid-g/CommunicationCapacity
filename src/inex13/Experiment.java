package inex13;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.naming.PartialResultException;

import inex09.InexIndexer;
import inex09.Utils;

public class Experiment {

	public static final String CONTENT_ATTRIB = "content";
	public static final String DOCNAME_ATTRIB = "name";
	public static final String TITLE_ATTRIB = "title";
	
	static final String DATA_SET = "/scratch/data-sets/inex_13/";
	static final String DATA_FOLDER = "data/";
	static final String INDEX_DIR = DATA_FOLDER + "index/";
	static final String QUERY_FILE = DATA_FOLDER + "queries/inex_ld/2013-ld-adhoc-topics.xml";
	static final String QREL_FILE = DATA_FOLDER + "queries/inex_ld/2013-ld-adhoc-qrels/2013LDT-adhoc.qrels";
	static final String RESULT_DIR = DATA_FOLDER + "result/inex13_dbsize/";
	
	static final String FILE_LIST = DATA_FOLDER + "filelist_proc.txt";

	public static void main(String[] args) {
		randomizedDbSize();
	}

	static void createFileList() {
		List<String> allFiles = Utils.listFilesForFolder(new File(DATA_SET));
		Collections.shuffle(allFiles);
		try {
			Files.write(Paths.get("data/filelist.txt"), allFiles,
					StandardCharsets.UTF_8, StandardOpenOption.CREATE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void randomizedDbSize() {
		System.out.println(new Date().toString() + " listing files..");
		int partitionNo = 10;
		List<String> allFiles = Utils.listFilesForFolder(new File(DATA_SET));
		Collections.shuffle(allFiles);
		System.out
				.println(new Date().toString() + " partitioning file names..");
		ArrayList<String[]> partitions = Utils.partitionArray(
				allFiles.toArray(new String[allFiles.size()]), partitionNo);
		String prevIndexPath = null;
		String indexPath[] = new String[partitionNo];
		for (int i = 0; i < partitionNo; i++)
			indexPath[i] = INDEX_DIR + "inex13_part" + i;

		for (int i = 0; i < partitionNo; i++) {
			System.out.println(new Date().toString() + " iteration " + i);
			Date index_t = new Date();
			System.out.println("indexing ");
			System.out.println("partition length: " + partitions.get(i).length);
			InexIndexer.buildIndex(partitions.get(i), indexPath[i], false);
			if (prevIndexPath != null) {
				System.out.println("updating index..");
				InexIndexer.updateIndex(prevIndexPath, indexPath[i]);
			}
			Date query_t = new Date();
			prevIndexPath = indexPath[i];
			System.out.println("indexing time "
					+ (query_t.getTime() - index_t.getTime()) / 60000 + "mins");
		}

		// running queries
		HashMap<Integer, InexQueryDAO> queriesMap = QueryParser.buildQueries(
				QUERY_FILE, QREL_FILE);
		List<InexQueryDAO> queries = new ArrayList<InexQueryDAO>();
		queries.addAll(queriesMap.values());
		// queries.add(queriesMap.get(2009002));
		System.out.println(new Date().toString() + " submitting queries");
		double gain[] = new double[partitionNo];
		double preMap[][] = new double[queries.size()][partitionNo];
		for (int i = 0; i < partitionNo; i++) {
			System.out.println("iteration " + i);
			List<InexQueryResult> resultList = QueryServices.runQueries(
					queries, indexPath[i]);
			for (int j = 0; j < resultList.size(); j++) {
				preMap[j][i] = resultList.get(j).precisionAtK(20);
				gain[i] += preMap[j][i];
			}
		}
		System.out.println(new Date().toString() + " queries are done!");
		System.out.println(Arrays.toString(gain));

		// writing results to file
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + "hanhan.csv");
			for (int i = 0; i < preMap.length; i++) {
				fw.write("\"" + queries.get(i).text + "\"" + ", ");
				for (int j = 0; j < preMap[0].length - 1; j++) {
					fw.write(preMap[i][j] + ", ");
				}
				fw.write(preMap[i][preMap[0].length - 1] + "\n");
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
