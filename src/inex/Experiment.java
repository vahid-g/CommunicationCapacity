package inex;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import inex_msn.InexIndexer;
import inex_msn.Utils;

public class Experiment {
	public static void main(String[] args) {
		
	}
	
	public static void buildPartitionedIndex() { // partitioning independent of related tuples
		List<String> allFiles = Utils.listFilesForFolder(new File("???"));
		Collections.shuffle(allFiles);
		int partitionNo = 10;
		ArrayList<String[]> partitions = Utils.partitionArray(
				allFiles.toArray(new String[allFiles.size()]), partitionNo);
		String prevIndexPath = null;
		for (int i = 0; i < partitionNo; i++) {
			String indexPath = "???" + i;
			System.out.println("iteration " + i);
			Date index_t = new Date();
			System.out.println("indexing ");
			System.out.println("partition length: " + partitions.get(i).length);
			InexIndexer.buildIndex(partitions.get(i), indexPath);
			if (prevIndexPath != null) {
				System.out.println("updating index..");
				InexIndexer.updateIndex(prevIndexPath, indexPath);
			}
			Date query_t = new Date();
			prevIndexPath = indexPath;
			System.out.println("indexing time "
					+ (query_t.getTime() - index_t.getTime()) / 60000 + "mins");
			
			// run queries
			System.out.println("querying time "
					+ (new Date().getTime() - query_t.getTime()) / 60000
					+ "mins");
		}
	}
	
	
}
