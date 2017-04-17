package indexing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import wiki_inex09.ClusterDirectoryInfo;
import wiki_inex13.ClusterExperiment13;

public class InexFileMetadata implements Comparable<InexFileMetadata> {
	
	public String path;
	public double weight;
	public String title;
	
	public InexFileMetadata(String path, double visitCount, String title) {
		super();
		this.path = path;
		this.weight = visitCount;
		this.title = title;
	}
	
	public InexFileMetadata(String path, double visitCount){
		this(path, visitCount, "");
	}
	
	@Override
	public int compareTo(InexFileMetadata o) {
		return Double.compare(o.weight, weight);
	}

	public static List<InexFileMetadata> loadFilePathCountTitle(
			String pathCountTitleFile) {
		ClusterExperiment13.LOGGER.log(Level.INFO, "Loading path-count-titles..");
		List<InexFileMetadata> pathCountList = new ArrayList<InexFileMetadata>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				pathCountTitleFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				try {
					if (!line.contains(","))
						continue;
					String[] fields = line.split(",");
					String path = ClusterDirectoryInfo.CLUSTER_BASE + fields[0];
					Integer count = Integer.parseInt(fields[1].trim());
					String title = fields[2].trim();
					pathCountList.add(new InexFileMetadata(path, count, title));
				} catch (Exception e) {
					ClusterExperiment13.LOGGER.log(Level.WARNING, "Couldn't read PathCountTitle: "
							+ line + " cause: " + e.toString());
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return pathCountList;
	}
	
}