package wiki13;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import popularity.PopularityUtils;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.cache_selection.ClarityScore;
import wiki13.cache_selection.JelinekMercerScore;
import wiki13.cache_selection.LanguageModelScore;
import wiki13.cache_selection.QueryDifficultyComputer;
import wiki13.cache_selection.SimpleCacheScore;
import wiki13.cache_selection.VarianceScore;
import wiki13.cache_selection.VarianceScore.VarianceScoreMode;

public class WikiQueryDifficultyExperiments {

	public static final Logger LOGGER = Logger.getLogger(WikiQueryDifficultyExperiments.class.getName());

	public static void main(String[] args) throws IOException {

		Options options = new Options();
		Option expNumberOption = new Option("exp", true, "Number of experiment");
		expNumberOption.setRequired(true);
		Option totalExpNumberOption = new Option("total", true, "Total number of experiments");
		totalExpNumberOption.setRequired(true);
		options.addOption(totalExpNumberOption);
		options.addOption(expNumberOption);
		Option difficultyOption = new Option("diff", true, "Flag to run difficulty experiment");
		difficultyOption.setRequired(true);
		options.addOption(difficultyOption);
		Option querysetOption = new Option("queryset", true, "specifies the query log (msn/inex)");
		querysetOption.setRequired(true);
		options.addOption(querysetOption);
		Option server = new Option("server", true, "Specifies maple/hpc");
		options.addOption(server);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			cl = clp.parse(options, args);
			WikiFilesPaths paths = null;
			if (cl.getOptionValue("server").equals("maple")) {
				paths = WikiFilesPaths.getMaplePaths();
			} else if (cl.getOptionValue("server").equals("hpc")) {
				paths = WikiFilesPaths.getHpcPaths();
			}
			int expNo = Integer.parseInt(cl.getOptionValue("exp"));
			int totalExp = Integer.parseInt(cl.getOptionValue("total"));
			String indexPath = paths.getIndexBase() + expNo;
			List<ExperimentQuery> queries;
			if (cl.getOptionValue("queryset").equals("msn")) {
				queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(), paths.getMsnQrelFilePath());
			} else if (cl.getOptionValue("queryset").equals("msn")) {
				queries = QueryServices.loadInexQueries(paths.getInexQueryFilePath(), paths.getInexQrelFilePath(),
						"title");
			} else {
				throw new org.apache.commons.cli.ParseException("Queryset is not recognized");
			}
			String difficultyMetric = cl.getOptionValue("diff");
			if (difficultyMetric.equals("pop")) {
				List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.15f);
				Map<String, Double> idPopMap = PopularityUtils.loadIdPopularityMap(paths.getAccessCountsPath());
				List<String> metric = new ArrayList<String>();
				for (QueryResult result : results) {
					double popSum = 0;
					double popSquaredSum = 0;
					for (int i = 0; i < Math.min(20, result.getTopDocuments().size()); i++) {
						double popularity = idPopMap.get(result.getTopDocuments().get(i).id);
						popSum += popularity;
						popSquaredSum += Math.pow(popularity, 2);
					}
					double ex = popSum / 20;
					metric.add(ex + ", " + ((popSquaredSum / 20) - Math.pow(ex, 2)));
				}
				try (FileWriter fw = new FileWriter(expNo + ".csv")) {
					for (int i = 0; i < results.size(); i++) {
						fw.write(results.get(i).query.getText() + ", " + metric.get(i) + "\n");
					}
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			} else {
				runCacheSelectionExperiment(expNo, totalExp, indexPath, queries, difficultyMetric, paths);
			}
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.INFO, e.getMessage());
			formatter.printHelp("", options);
			return;
		}
	}

	public static void runCacheSelectionExperiment(int expNo, int totalExp, String indexPath,
			List<ExperimentQuery> queries, String difficultyMetric, WikiFilesPaths paths)
			throws ParseException, IOException {
		LOGGER.log(Level.INFO, "querylog size " + queries.size());
		QueryDifficultyComputer qdc;
		IndexReader globalReader = null;
		if (difficultyMetric.equals("scs")) {
			qdc = new QueryDifficultyComputer(new ClarityScore());
		} else if (difficultyMetric.equals("maxvar")) {
			qdc = new QueryDifficultyComputer(new VarianceScore(VarianceScoreMode.MAX_VARIANCE));
		} else if (difficultyMetric.equals("avgvar")) {
			qdc = new QueryDifficultyComputer(new VarianceScore(VarianceScoreMode.AVERAGE_VARIANCE));
		} else if (difficultyMetric.equals("maxex")) {
			qdc = new QueryDifficultyComputer(new VarianceScore(VarianceScoreMode.MAX_EX));
		} else if (difficultyMetric.equals("avgex")) {
			qdc = new QueryDifficultyComputer(new VarianceScore(VarianceScoreMode.AVERAGE_EX));
		} else if (difficultyMetric.equals("lm")) {
			qdc = new QueryDifficultyComputer(new LanguageModelScore());
		} else if (difficultyMetric.equals("simple")) {
			qdc = new QueryDifficultyComputer(new SimpleCacheScore());
		} else if (difficultyMetric.equals("jms")) {
			globalReader = DirectoryReader.open(FSDirectory.open(Paths.get(paths.getIndexBase() + totalExp)));
			qdc = new QueryDifficultyComputer(new JelinekMercerScore(globalReader));
		} else {
			throw new org.apache.commons.cli.ParseException("Difficulty metric needs to be specified");
		}
		// Map<String, Double> titleDifficulties = qdc.computeQueryDifficulty(indexPath,
		// queries,
		// WikiFileIndexer.TITLE_ATTRIB);
		queries = queries.subList(0, 100);
		long startTime = System.currentTimeMillis();
		Map<String, Double> contentDifficulties = qdc.computeQueryDifficulty(indexPath, queries,
				WikiFileIndexer.CONTENT_ATTRIB);
		long endTime = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "Time spent for RS per query = " + (endTime - startTime) / queries.size() + " (ms)");
		if (globalReader != null) {
			try {
				globalReader.close();
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		// WikiExperimentHelper.writeMapToFile(titleDifficulties, "title_diff_" + expNo
		// + ".csv");
		WikiExperimentHelper.writeMapToFile(contentDifficulties, "content_diff_" + expNo + ".csv");
	}
}
