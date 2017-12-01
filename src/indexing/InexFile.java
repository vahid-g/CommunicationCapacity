package indexing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;

public class InexFile {

    public String id;
    public String path;
    public double weight;
    public String title;

    static final Logger LOGGER = Logger.getLogger(InexFile.class.getName());

    public static class ReverseWeightComparator implements Comparator<InexFile> {
	@Override
	public int compare(InexFile o1, InexFile o2) {
	    return Double.compare(o2.weight, o1.weight);
	}
    }

    public InexFile(String path, double visitCount, String title) {
	super();
	this.path = path;
	this.weight = visitCount;
	this.title = title;
    }

    public InexFile(String path, double visitCount) {
	this(path, visitCount, "");
    }

    // can parse inex file path csv file with 2 or 3 fields
    public static List<InexFile> loadInexFileList(String pathCountTitleFile) {
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
		    if (fields.length == 3) {
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
	LOGGER.log(Level.INFO,
		"Size of List<InexFile>: " + pathCountList.size());
	return pathCountList;
    }

    public static Map<String, InexFile> loadFilePathCountTitleMap(
	    String pathCountTitleFile) {
	List<InexFile> fileList = InexFile.loadInexFileList(pathCountTitleFile);
	HashMap<String, InexFile> idToInexFile = new HashMap<String, InexFile>();
	for (InexFile file : fileList) {
	    idToInexFile.put(FilenameUtils.removeExtension(new File(file.path)
		    .getName()), file);
	}
	return idToInexFile;
    }

}