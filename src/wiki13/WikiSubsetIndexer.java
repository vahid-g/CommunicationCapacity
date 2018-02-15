package wiki13;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import wiki13.cluster.WikiClusterPaths;
import wiki13.maple.WikiMaplePaths;

public class WikiSubsetIndexer {

	public static final Logger LOGGER = Logger.getLogger(WikiSubsetIndexer.class.getName());

	public static void main(String[] args) {
		Options options = new Options();
		Option totalExpNumberOption = new Option("total", true, "Total number of partitions");
		totalExpNumberOption.setRequired(true);
		options.addOption(totalExpNumberOption);
		Option expNumberOption = new Option("exp", true, "Number of partition to index");
		expNumberOption.setRequired(true);
		options.addOption(expNumberOption);
		Option server = new Option("server", true, "Specifies maple/hpc");
		options.addOption(server);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;
		try {
			cl = clp.parse(options, args);
			int totalPartitionCount = Integer.parseInt(cl.getOptionValue("total"));
			int partitionNumber = Integer.parseInt(cl.getOptionValue("exp"));
			String indexPath = "";
			String accessCountsFilePath = "";
			if (cl.getOptionValue("server").equals("hpc")) {
				indexPath = WikiClusterPaths.INDEX_BASE + "wiki13_p" + totalPartitionCount + "_w09" + "/"
						+ partitionNumber;
				accessCountsFilePath = WikiClusterPaths.FILELIST_PATH_COUNT09;
			} else if (cl.getOptionValue("server").equals("maple")) {
				indexPath = WikiMaplePaths.INDEX_BASE + partitionNumber;
				accessCountsFilePath = WikiMaplePaths.FILELIST_COUNT09_PATH;
			} else {
				throw new org.apache.commons.cli.ParseException("Server name is not valid");
			}
			LOGGER.log(Level.INFO, "Building index for partition {0}/{1}",
					new Object[] { partitionNumber, totalPartitionCount });
			long startTime = System.currentTimeMillis();
			WikiExperimentHelper.buildGlobalIndex(partitionNumber, totalPartitionCount, accessCountsFilePath,
					indexPath);
			long endTime = System.currentTimeMillis();
			LOGGER.log(Level.INFO, "Indexing time: {0} sec", (endTime - startTime) / 1000);
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.INFO, e.getMessage());
			formatter.printHelp("", options);
			return;
		}
	}
}
