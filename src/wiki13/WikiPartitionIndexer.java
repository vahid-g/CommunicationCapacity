package wiki13.cluster;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import wiki13.WikiExperiment;

public class WikiPartitionIndexer {

    public static final Logger LOGGER = Logger
	    .getLogger(WikiPartitionIndexer.class.getName());

    public static void main(String[] args) {
	Options options = new Options();
	Option totalExpNumberOption = new Option("total", true,
		"Total number of partitions");
	totalExpNumberOption.setRequired(true);
	options.addOption(totalExpNumberOption);
	Option expNumberOption = new Option("exp", true,
		"Number of partition to index");
	expNumberOption.setRequired(true);
	options.addOption(expNumberOption);
	CommandLineParser clp = new DefaultParser();
	HelpFormatter formatter = new HelpFormatter();
	CommandLine cl;

	try {
	    cl = clp.parse(options, args);
	    int partitionNumber = Integer.parseInt(cl.getOptionValue("exp"));
	    int totalPartitionCount = Integer
		    .parseInt(cl.getOptionValue("total"));
	    String indexPath = WikiClusterPaths.INDEX_BASE + "wiki13_p"
		    + totalPartitionCount + "_w09" + "/part_" + partitionNumber;
	    LOGGER.log(Level.INFO, "Building index for partition {0}/{1}",
		    new Object[] { partitionNumber, totalPartitionCount });
	    long startTime = System.currentTimeMillis();
	    WikiExperiment.buildGlobalIndex(partitionNumber,
		    totalPartitionCount, WikiClusterPaths.FILELIST_PATH_COUNT09,
		    indexPath);
	    long endTime = System.currentTimeMillis();
	    LOGGER.log(Level.INFO, "Indexing time: {0} sec",
		    (endTime - startTime) / 1000);
	} catch (org.apache.commons.cli.ParseException e) {
	    LOGGER.log(Level.INFO, e.getMessage());
	    formatter.printHelp("", options);
	    return;
	}
    }

}
