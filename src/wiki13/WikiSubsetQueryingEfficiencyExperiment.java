package wiki13;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import query.ExperimentQuery;
import query.QueryServices;

public class WikiSubsetQueryingEfficiencyExperiment {

	private static final Logger LOGGER = Logger.getLogger(WikiSubsetQueryingEfficiencyExperiment.class.getName());
	private static WikiFilesPaths PATHS = WikiFilesPaths.getMaplePaths();

	public static void main(String[] args) {
		Options options = new Options();
		Option expOption = new Option("exp", true, "experiment number");
		expOption.setRequired(true);
		options.addOption(expOption);
		Option querysetOption = new Option("queryset", true, "specifies the query log (msn/inex)");
		querysetOption.setRequired(true);
		options.addOption(querysetOption);
		Option gammaOption = new Option("gamma", true, "the weight of title field");
		options.addOption(gammaOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;
		try {
			cl = clp.parse(options, args);
			int partitionCount = 100;
			List<ExperimentQuery> queries;
			float gamma = Float.parseFloat(cl.getOptionValue("gamma", "0.15f"));
			if (cl.getOptionValue("queryest").equals("msn")) {
				queries = QueryServices.loadMsnQueries(PATHS.getMsnQueryFilePath(), PATHS.getMsnQrelFilePath());
			} else {
				queries = QueryServices.loadInexQueries(PATHS.getInexQrelFilePath(), PATHS.getInexQrelFilePath(),
						"title");
			}
			queries = queries.subList(0, 100);
			double times[] = new double[partitionCount];
			int iterationCount = 10;
			for (int i = 0; i < iterationCount; i++) {
				for (int expNo = 1; expNo <= partitionCount; expNo++) {
					String indexPath = PATHS.getIndexBase() + expNo;
					long startTime = System.currentTimeMillis();
					WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, gamma);
					long spentTime = System.currentTimeMillis() - startTime;
					times[expNo - 1] += spentTime;
				}
			}
			try (FileWriter fw = new FileWriter("time_results.csv")) {
				for (double l : times) {
					fw.write(l / iterationCount + "\n");
				}
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.SEVERE, e.getMessage());
			formatter.printHelp("", options);
		}
	}

}
