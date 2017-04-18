package indexing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import wiki_inex13.Wiki13Experiment;

public class InexFile implements Comparable<InexFile> {
	
	public String path;
	public double weight;
	public String title;
	
	static final Logger LOGGER = Logger.getLogger(InexFile.class
			.getName());
	
	public InexFile(String path, double visitCount, String title) {
		super();
		this.path = path;
		this.weight = visitCount;
		this.title = title;
	}
	
	public InexFile(String path, double visitCount){
		this(path, visitCount, "");
	}
	
	@Override
	public int compareTo(InexFile o) {
		return Double.compare(o.weight, weight);
	}

	public static List<InexFile> loadFilePathCountTitle(
			String pathCountTitleFile) {
		Wiki13Experiment.LOGGER.log(Level.INFO, "Loading path-count-titles..");
		List<InexFile> pathCountList = new ArrayList<InexFile>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				pathCountTitleFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				try {
					if (!line.contains(","))
						continue;
					String[] fields = line.split(",");
					String path = fields[0];
					Double count = Double.parseDouble(fields[1].trim());
					if (fields.length == 3){
						String title = fields[2].trim();
						pathCountList.add(new InexFile(path, count, title));
					} else {
						pathCountList.add(new InexFile(path, count));
					}
				} catch (Exception e) {
					LOGGER.log(Level.WARNING, "Couldn't read PathCountTitle: "
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