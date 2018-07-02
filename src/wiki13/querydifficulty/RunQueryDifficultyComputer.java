package wiki13.querydifficulty;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import cache_enhancement.Similarity;
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
import wiki13.WikiExperimentHelper;
import wiki13.WikiFileIndexer;
import wiki13.WikiFilesPaths;
import wiki13.querydifficulty.VarianceScore.VarianceScoreMode;

public class RunQueryDifficultyComputer {

	public static final Logger LOGGER = Logger.getLogger(RunQueryDifficultyComputer.class.getName());

	public static void main(String[] args) throws IOException {

		Options options = new Options();
		Option expNumberOption = new Option("exp", true, "Number of experiment");
		expNumberOption.setRequired(true);
		options.addOption(expNumberOption);
		Option totalExpNumberOption = new Option("total", true, "Total number of experiments");
		totalExpNumberOption.setRequired(true);
		options.addOption(totalExpNumberOption);
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
			String indexName = cl.getOptionValue("exp");
			int totalExp = Integer.parseInt(cl.getOptionValue("total"));
			String indexPath = paths.getIndexBase() + indexName;
			String globalIndexPath = paths.getIndexBase() + totalExp;
			List<ExperimentQuery> queries;
			String queryset = cl.getOptionValue("queryset", "none");
			if (queryset.equals("msn")) {
				queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(), paths.getMsnQrelFilePath());
			} else if (queryset.equals("inex")) {
				queries = QueryServices.loadInexQueries(paths.getInexQueryFilePath(), paths.getInexQrelFilePath(),
						"title");
			} else {
				throw new org.apache.commons.cli.ParseException("Queryset is not recognized");
			}
			String difficultyMetric = cl.getOptionValue("diff");
			List<String> scores = null;
			RunQueryDifficultyComputer wqde = new RunQueryDifficultyComputer();
			if (difficultyMetric.equals("pop")) {
				List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.15f);
				scores = wqde.runQueryPopularityScoreComputer(paths, results);
			} else {
				scores = wqde.runQueryScoreComputer(indexPath, globalIndexPath, queries, difficultyMetric);
			}
			try (FileWriter fw = new FileWriter(queryset + "_" + indexName + ".csv")) {
				for (int i = 0; i < queries.size(); i++) {
					fw.write(queries.get(i).getText() + ", " + scores.get(i) + "\n");
				}
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			formatter.printHelp("", options);
			return;
		}
	}

	public List<String> runQueryScoreComputer(String indexPath, String globalIndexPath, List<ExperimentQuery> queries,
			String difficultyMetric) throws ParseException, IOException {
		LOGGER.log(Level.INFO, "querylog size " + queries.size());
		QueryDifficultyComputer qdc;
		List<String> scores = new ArrayList<String>();
		try (IndexReader globalReader = DirectoryReader.open(FSDirectory.open(Paths.get(globalIndexPath)))) {
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
				qdc = new QueryDifficultyComputer(new JelinekMercerScore(globalReader));
			} else if (difficultyMetric.equals("bjms")) {
				qdc = new QueryDifficultyComputer(new BigramJelinekMercerScore(globalReader));
			} else {
				throw new org.apache.commons.cli.ParseException("Difficulty metric needs to be specified");
			}
			long startTime = System.currentTimeMillis();
			Map<String, Double> contentDifficulties = qdc.computeQueryDifficulty(indexPath, queries,
					WikiFileIndexer.CONTENT_ATTRIB);
			long endTime = System.currentTimeMillis();
			LOGGER.log(Level.INFO, "Time spent for RS per query = " + (endTime - startTime) / queries.size() + " (ms)");
			for (ExperimentQuery query : queries) {
				scores.add(Double.toString(contentDifficulties.get(query.getText())));
			}
		}
		return scores;
	}

	public List<String> runQueryPopularityScoreComputer(WikiFilesPaths paths, List<QueryResult> results) {
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
		return metric;
	}
}
