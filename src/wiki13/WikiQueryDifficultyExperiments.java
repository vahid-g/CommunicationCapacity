package wiki13;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import popularity.PopularityUtils;
import query.ExperimentQuery;
import query.LuceneQueryBuilder;
import query.QueryResult;
import query.QueryServices;
import wiki13.cache_selection.BigramJelinekMercerScore;
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
			int expNo = Integer.parseInt(cl.getOptionValue("exp"));
			int totalExp = Integer.parseInt(cl.getOptionValue("total"));
			String indexPath = paths.getIndexBase() + expNo;
			List<ExperimentQuery> queries;
			if (cl.getOptionValue("queryset").equals("msn")) {
				queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(), paths.getMsnQrelFilePath());
			} else if (cl.getOptionValue("queryset").equals("inex")) {
				queries = QueryServices.loadInexQueries(paths.getInexQueryFilePath(), paths.getInexQrelFilePath(),
						"title");
			} else {
				throw new org.apache.commons.cli.ParseException("Queryset is not recognized");
			}
			String difficultyMetric = cl.getOptionValue("diff");
			List<String> scores = null;
			WikiQueryDifficultyExperiments wqde = new WikiQueryDifficultyExperiments();
			if (difficultyMetric.equals("pop")) {
				List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.15f);
				scores = wqde.runQueryPopularityScoreComputer(paths, results);
			} else if (difficultyMetric.equals("???")) {

			} else {
				String globalIndexPath = paths.getIndexBase() + totalExp;
				scores = wqde.runQueryScoreComputer(indexPath, globalIndexPath, queries, difficultyMetric);
			}
			try (FileWriter fw = new FileWriter(expNo + ".csv")) {
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

	List<String> runQueryScoreComputer(String indexPath, String globalIndexPath, List<ExperimentQuery> queries,
			String difficultyMetric) throws ParseException, IOException {
		LOGGER.log(Level.INFO, "querylog size " + queries.size());
		QueryDifficultyComputer qdc;
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
			try (IndexReader globalReader = DirectoryReader.open(FSDirectory.open(Paths.get(globalIndexPath)))) {
				qdc = new QueryDifficultyComputer(new JelinekMercerScore(globalReader));
			}
		} else if (difficultyMetric.equals("bjms")) {
			try (IndexReader globalReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
				qdc = new QueryDifficultyComputer(new BigramJelinekMercerScore(globalReader));
			}
		} else {
			throw new org.apache.commons.cli.ParseException("Difficulty metric needs to be specified");
		}
		long startTime = System.currentTimeMillis();
		Map<String, Double> contentDifficulties = qdc.computeQueryDifficulty(indexPath, queries,
				WikiFileIndexer.CONTENT_ATTRIB);
		long endTime = System.currentTimeMillis();
		LOGGER.log(Level.INFO, "Time spent for RS per query = " + (endTime - startTime) / queries.size() + " (ms)");
		List<String> scores = new ArrayList<String>();
		for (ExperimentQuery query : queries) {
			scores.add(Double.toString(contentDifficulties.get(query.getText())));
		}
		return scores;
	}

	List<String> runQueryPopularityScoreComputer(WikiFilesPaths paths, List<QueryResult> results) {
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

	List<String> runQueryFeatureExtraction(String indexPath, List<ExperimentQuery> queries) {
		List<String> scores = new ArrayList<String>();
		try (IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
			IndexSearcher searcher = new IndexSearcher(indexReader);
			searcher.setSimilarity(new BM25Similarity());
			Map<String, Float> fieldToBoost = new HashMap<String, Float>();
			fieldToBoost.put(WikiFileIndexer.TITLE_ATTRIB, 0.1f);
			fieldToBoost.put(WikiFileIndexer.CONTENT_ATTRIB, 0.9f);
			LuceneQueryBuilder lqb = new LuceneQueryBuilder(fieldToBoost);
			for (ExperimentQuery query : queries) {
				String[] terms = query.getText().split(" ");
				Stream<String> termsStream = Arrays.stream(terms).map(t -> t.toLowerCase());
				double titleCoveredQueryTermRatio = coveredQueryTermRatio(indexReader, termsStream,
						WikiFileIndexer.TITLE_ATTRIB);
				double contentCoveredQueryTermRatio = coveredQueryTermRatio(indexReader, termsStream,
						WikiFileIndexer.CONTENT_ATTRIB);
				// IR scores
				// same metrics for bigrams
				// popularity scores?
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return scores;
	}

	private double coveredQueryTermRatio(IndexReader indexReader, Stream<String> tokenStream, String field) {
		Stream<Term> termsStream = tokenStream.map(t -> new Term(field, t));
		long coveredTermCount = termsStream.map(t -> {
			long covered = 0;
			try {
				covered = indexReader.totalTermFreq(t);
			} catch (IOException ioe) {
				return 0;
			}
			return covered > 0 ? 1 : 0;
		}).mapToInt(Integer::intValue).sum();
		return coveredTermCount / (double) termsStream.count();
	}

	private double meanNormalizedDocumentFrequency(IndexReader indexReader, Stream<String> tokenStream, String field) {
		Stream<Term> termStream = tokenStream.map(t -> new Term(field, t));
		double dfSum = termStream.map(t -> {
			long df = 0;
			int n = 1;
			try {
				df = indexReader.docFreq(t);
				n = indexReader.getDocCount(field);
			} catch (IOException ioe) {
				df = 0;
				n = 1;
			}
			return df / (double) n;
		}).mapToDouble(Double::doubleValue).sum();
		return dfSum / termStream.count();
	}

	private double minNormalizedDocumentFrequency(IndexReader indexReader, Stream<String> tokenStream, String field) {
		Stream<Term> termStream = tokenStream.map(t -> new Term(field, t));
		double dfMin = termStream.map(t -> {
			long df = 0;
			int n = 1;
			try {
				df = indexReader.docFreq(t);
				n = indexReader.getDocCount(field);
			} catch (IOException ioe) {
				df = 0;
				n = 1;
			}
			return df / (double) n;
		}).mapToDouble(Double::doubleValue).min().orElse(0.0);
		return dfMin;
	}

	private double docsWithAllBiwords() {
		// TODO continue here
		return 0;
	}

	private double meanBM25Score(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
		Query query = lqb.buildQuery(queryText);
		ScoreDoc[] scoreDocHits = null;
		int k = 20;
		try {
			scoreDocHits = searcher.search(query, k).scoreDocs;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (scoreDocHits != null)
			return Arrays.stream(scoreDocHits).map(h -> h.score).reduce(Float::sum).orElse(0f)
					/ (double) Math.max(1, Math.min(scoreDocHits.length, k));
		else
			return 0;
	}

	private double minBM25Score(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
		Query query = lqb.buildQuery(queryText);
		ScoreDoc[] scoreDocHits = null;
		int k = 20;
		try {
			scoreDocHits = searcher.search(query, k).scoreDocs;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (scoreDocHits != null)
			return Arrays.stream(scoreDocHits).map(h -> h.score).reduce(Float::min).orElse(0f)
					/ (double) Math.max(1, Math.min(scoreDocHits.length, k));
		else
			return 0;
	}

	private double meanBoolScore(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
		Query query = lqb.buildQuery(queryText, Operator.AND);
		ScoreDoc[] scoreDocHits = null;
		int k = 20;
		try {
			scoreDocHits = searcher.search(query, k).scoreDocs;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (scoreDocHits != null)
			return Arrays.stream(scoreDocHits).map(h -> h.score).reduce(Float::sum).orElse(0f)
					/ (double) Math.max(1, Math.min(scoreDocHits.length, k));
		else
			return 0;
	}

	private double minBoolScore(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
		Query query = lqb.buildQuery(queryText, Operator.AND);
		ScoreDoc[] scoreDocHits = null;
		int k = 20;
		try {
			scoreDocHits = searcher.search(query, k).scoreDocs;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (scoreDocHits != null)
			return Arrays.stream(scoreDocHits).map(h -> h.score).reduce(Float::min).orElse(0f)
					/ (double) Math.max(1, Math.min(scoreDocHits.length, k));
		else
			return 0;
	}
}
