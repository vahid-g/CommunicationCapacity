package wiki13.maple;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperiment;

public class WikiMapleCachingExperiment {

	private static final Logger LOGGER = Logger
			.getLogger(WikiMapleCachingExperiment.class.getName());

	public static void main(String[] args) {
		Options options = new Options();
		Option indexOption = new Option("index", false, "run indexing mode");
		options.addOption(indexOption);
		Option queryOption = new Option("query", false, "run querying");
		options.addOption(queryOption);
		Option filterOption = new Option("filter", false,
				"enables filtering mode");
		options.addOption(filterOption);
		Option cacheOption = new Option("cache", false, "enables caching mode");
		options.addOption(cacheOption);
		Option useMsnOption = new Option("msn", false,
				"specifies the query log (msn/inex)");
		options.addOption(useMsnOption);
		Option partitionsOption = new Option("total", true,
				"number of partitions");
		options.addOption(partitionsOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			String indexPath = WikiMapleExperiment.DATA_PATH + "wiki_index/";
			cl = clp.parse(options, args);
			int partitionCount = Integer.parseInt(cl.getOptionValue("total",
					"100"));
			if (cl.hasOption("index")) {
				for (int i = 1; i <= partitionCount; i++) {
					WikiExperiment.buildGlobalIndex(i, partitionCount,
							WikiMapleExperiment.FILELIST_PATH, indexPath + i);
				}
			}
			if (cl.hasOption("query")) {
				List<ExperimentQuery> queries;
				if (cl.hasOption("msn")) {
					queries = QueryServices.loadMsnQueries(
							WikiMapleExperiment.MSN_QUERY_FILE_PATH,
							WikiMapleExperiment.MSN_QREL_FILE_PATH);
				} else {
					queries = QueryServices.loadInexQueries(
							WikiMapleExperiment.QUERY_FILE_PATH,
							WikiMapleExperiment.QREL_FILE_PATH, "title");
				}
				for (int i = 1; i <= partitionCount; i++) {
					List<QueryResult> results = WikiExperiment
							.runQueriesOnGlobalIndex(indexPath + i, queries,
									0.15f);
					WikiExperiment.writeResultsToFile(results, "result/" + i
							+ ".csv");
				}
			}
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.INFO, e.getMessage());
			formatter.printHelp("", options);
		}
	}

	public static List<Double> computeQueryDifficulty(String indexPath,
			List<ExperimentQuery> queries, String field) {
		List<Double> difficulties = new ArrayList<Double>();
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexPath)))) {
			computeQueryDifficulty(reader, queries, field);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return difficulties;
	}

	public static List<Double> computeQueryDifficulty(IndexReader reader,
			List<ExperimentQuery> queries, String field) throws IOException {
		List<Double> difficulties = new ArrayList<Double>();
		long titleTermCount = reader.getSumTotalTermFreq(field);
		LOGGER.log(Level.INFO, "Total number of terms in " + field + ": "
				+ titleTermCount);
		for (ExperimentQuery query : queries) {
			List<String> terms = Arrays
					.asList(query.getText().split("[ \"'+]")).stream()
					.filter(str -> !str.isEmpty()).collect(Collectors.toList());
			int qLength = terms.size();
			long termCountSum = 0;
			for (String term : terms) {
				System.out.println("term = \"" + term + "\"");
				termCountSum += reader.totalTermFreq(new Term(field, term));
				System.out.println("count = " + termCountSum);
			}
			double ictf = Math.log(titleTermCount / (termCountSum + 1.0));
			difficulties.add(1.0 / qLength + ictf / qLength);
		}
		return difficulties;
	}

}
