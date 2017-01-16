package inex09;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
	
	public static void experiment0() { // partitioning independent of related tuples
		List<String> allFiles = Utils.listFilesForFolder(new File("???"));
		Collections.shuffle(allFiles);
		int partitionNo = 10;
		ArrayList<String[]> partitions = Utils.partitionArray(
				allFiles.toArray(new String[allFiles.size()]), partitionNo);
		String prevExperiment = null;
		
		for (int i = 0; i < partitionNo; i++) {
			System.out.println("iteration " + i);
			Date index_t = new Date();
			System.out.println("indexing ");
			System.out.println("partition length: " + partitions.get(i).length);
			InexIndexer.buildIndex(partitions.get(i), "???");
			if (prevExperiment != null) {
				System.out.println("updating index..");
				InexIndexer.updateIndex("???", "???");
			}
			Date query_t = new Date();
			prevExperiment = "???";
			System.out.println("indexing time "
					+ (query_t.getTime() - index_t.getTime()) / 60000 + "mins");
			
			System.out.println("querying time "
					+ (new Date().getTime() - query_t.getTime()) / 60000
					+ "mins");
			
			
		}
	}

}