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
	private static WikiFilesPaths PATHS = WikiFilesPaths.getHpcPaths();

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
		Option useMsnQueryLogOption = new Option("msn", false, "specifies the query log (msn/inex)");
		options.addOption(useMsnQueryLogOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			cl = clp.parse(options, args);
			int expNo = Integer.parseInt(cl.getOptionValue("exp"));
			int totalExp = Integer.parseInt(cl.getOptionValue("total"));
			String indexPath = PATHS.getIndexBase() + expNo;
			List<ExperimentQuery> queries;
			if (cl.hasOption("msn")) {
				queries = QueryServices.loadMsnQueries(PATHS.getMsnQueryFilePath(), PATHS.getMsnQrelFilePath());
			} else {
				queries = QueryServices.loadInexQueries(PATHS.getInexQueryFilePath(), PATHS.getInexQrelFilePath(),
						"title");
			}
			String difficultyMetric = cl.getOptionValue("diff");
			if (difficultyMetric.equals("pop")) {
				List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.15f);
				Map<String, Double> idPopMap = PopularityUtils.loadIdPopularityMap(PATHS.getAccessCountsPath());
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
				runCacheSelectionExperiment(expNo, totalExp, indexPath, queries, difficultyMetric);
			}
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.INFO, e.getMessage());
			formatter.printHelp("", options);
			return;
		}
	}

	public static void runCacheSelectionExperiment(int expNo, int totalExp, String indexPath,
			List<ExperimentQuery> queries, String difficultyMetric) throws ParseException, IOException {
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
			globalReader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.getIndexBase() + totalExp)));
			qdc = new QueryDifficultyComputer(new JelinekMercerScore(globalReader));
		} else {
			throw new org.apache.commons.cli.ParseException("Difficulty metric needs to be specified");
		}
		Map<String, Double> titleDifficulties = qdc.computeQueryDifficulty(indexPath, queries,
				WikiFileIndexer.TITLE_ATTRIB);
		Map<String, Double> contentDifficulties = qdc.computeQueryDifficulty(indexPath, queries,
				WikiFileIndexer.CONTENT_ATTRIB);
		if (globalReader != null) {
			try {
				globalReader.close();
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		WikiExperimentHelper.writeMapToFile(titleDifficulties, "title_diff_" + expNo + ".csv");
		WikiExperimentHelper.writeMapToFile(contentDifficulties, "content_diff_" + expNo + ".csv");
	}
}
