package freebase;

import java.io.FileWriter;
import java.io.IOException;

public class FreebaseClusterExperiment {

	final static String CLUSTER_DATA_FOLDER = "/scratch/cluster-share/ghadakcv/";
	final static String CLUSTER_INDEX = FreebaseExperiment.DATA_FOLDER
			+ "index/";
	final static String CLUSTER_RESULTS = "result/";

	public static void main(String[] args) {
		int expNo = Integer.parseInt(args[0]);
		FreebaseClusterExperiment.experiment_randomizedDatabaseSizeQuerySize(
				expNo, 3, 10, "tbl_all");
	}

	// randomized database size query size experiment
	// output: two files with rows as query set instances and columns as
	// database instances showing number of queries that gained and lost p@3
	public static void experiment_randomizedDatabaseSizeQuerySize(int expNo,
			int qCount, int dCount, String tableName) {
		FreebaseExperiment.ExperimentResult fr = new FreebaseExperiment.ExperimentResult();
		fr.lostCount = new int[qCount][dCount];
		fr.foundCount = new int[qCount][dCount];
		fr = FreebaseExperiment.experiment_randomizedDatabaseSizeQuerySize(
				expNo, qCount, dCount, CLUSTER_INDEX, tableName);
		FileWriter fw = null;
		try {
			fw = new FileWriter(CLUSTER_RESULTS + tableName + "_lost_" + expNo
					+ ".csv");
			for (int i = 0; i < qCount; i++) {
				for (int j = 0; j < dCount - 1; j++) {
					fw.write(fr.lostCount[i][j] + ",");
				}
				fw.write(fr.lostCount[i][dCount - 1] + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			fw = new FileWriter(CLUSTER_RESULTS + tableName + "_found_" + expNo
					+ ".csv");
			for (int i = 0; i < qCount; i++) {
				for (int j = 0; j < dCount - 1; j++) {
					fw.write(fr.foundCount[i][j] + ",");
				}
				fw.write(fr.foundCount[i][dCount - 1] + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}
