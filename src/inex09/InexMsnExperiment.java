package inex09;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class InexMsnExperiment {
	
	public static void main(String[] args) {

		// buildIndex("/scratch/data-sets/inex_09/000", "data/index/inex09");
		
		List<MsnQuery> queries = InexQueryServices.loadMsnQueries(
				"data/queries/msn/query_qid.csv",
				"data/queries/msn/qid_qrel.csv");
		
		List<MsnQueryResult> results = InexQueryServices.runMsnQueries(queries, "data/index/inex09");
		try (FileWriter fw = new FileWriter("data/result/inex_msn.csv")) {
			for (MsnQueryResult mqr : results){
				fw.write(mqr.msnQuery.text + ", " + mqr.precisionAtK(3) + ", " + mqr.mrr() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void buildIndex(String datasetDir, String indexPath) {
		List<String> allFiles = Utils.listFilesForFolder(new File(datasetDir));
		InexIndexer.buildIndex(allFiles.toArray(new String[0]), indexPath);
	}

}