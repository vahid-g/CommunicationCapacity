package wiki13;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
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

public class WikiCacheSelectionExperiments {

	public static final Logger LOGGER = Logger.getLogger(WikiCacheSelectionExperiments.class.getName());

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
			WikiCacheSelectionExperiments wqde = new WikiCacheSelectionExperiments();
			if (difficultyMetric.equals("pop")) {
				List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.15f);
				scores = wqde.runQueryPopularityScoreComputer(paths, results);
			} else if (difficultyMetric.equals("feats")) {
				scores = wqde.runQueryFeatureExtraction(indexPath, queries);
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
			IndexSearcher booleanSearcher = new IndexSearcher(indexReader);
			booleanSearcher.setSimilarity(new BooleanSimilarity());
			Map<String, Float> fieldToBoost = new HashMap<String, Float>();
			fieldToBoost.put(WikiFileIndexer.TITLE_ATTRIB, 0.1f);
			fieldToBoost.put(WikiFileIndexer.CONTENT_ATTRIB, 0.9f);
			LuceneQueryBuilder lqb = new LuceneQueryBuilder(fieldToBoost);
			for (ExperimentQuery query : queries) {
				String[] terms = query.getText().toLowerCase().split(" ");
				List<Double> features = new ArrayList<Double>();
				features.add(coveredQueryTermRatio(indexReader, terms, WikiFileIndexer.TITLE_ATTRIB));
				features.add(coveredQueryTermRatio(indexReader, terms, WikiFileIndexer.CONTENT_ATTRIB));
				features.add(meanNormalizedDocumentFrequency(indexReader, terms, WikiFileIndexer.TITLE_ATTRIB));
				features.add(meanNormalizedDocumentFrequency(indexReader, terms, WikiFileIndexer.CONTENT_ATTRIB));
				features.add(minNormalizedDocumentFrequency(indexReader, terms, WikiFileIndexer.TITLE_ATTRIB));
				features.add(minNormalizedDocumentFrequency(indexReader, terms, WikiFileIndexer.CONTENT_ATTRIB));
				features.add(coveredBiwordRatio(searcher, query.getText(), WikiFileIndexer.TITLE_ATTRIB));
				features.add(coveredBiwordRatio(searcher, query.getText(), WikiFileIndexer.CONTENT_ATTRIB));
				features.add(
						meanNormalizedDocumentBiwordFrequency(searcher, query.getText(), WikiFileIndexer.TITLE_ATTRIB));
				features.add(meanNormalizedDocumentBiwordFrequency(searcher, query.getText(),
						WikiFileIndexer.CONTENT_ATTRIB));
				features.add(meanBM25Score(searcher, query.getText(), lqb));
				features.add(minBM25Score(searcher, query.getText(), lqb));
				features.add(meanBoolScore(booleanSearcher, query.getText(), lqb));
				features.add(minBoolScore(booleanSearcher, query.getText(), lqb));
				features.add(meanAverageTermDocPopularity(searcher, query.getText(), WikiFileIndexer.TITLE_ATTRIB));
				features.add(meanAverageTermDocPopularity(searcher, query.getText(), WikiFileIndexer.CONTENT_ATTRIB));
				scores.add(features.stream().map(f -> f + ",").collect(Collectors.joining()));
			}

		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return scores;
	}

	protected double coveredQueryTermRatio(IndexReader indexReader, String[] queryTerms, String field) {
		Stream<Term> termsStream = Arrays.stream(queryTerms).map(t -> new Term(field, t));
		long coveredTermCount = termsStream.map(t -> {
			long covered = 0;
			try {
				covered = indexReader.totalTermFreq(t);
			} catch (IOException ioe) {
				return 0;
			}
			return covered > 0 ? 1 : 0;
		}).mapToInt(Integer::intValue).sum();
		return coveredTermCount / (double) queryTerms.length;
	}

	protected double meanNormalizedDocumentFrequency(IndexReader indexReader, String[] queryTerms, String field) {
		Stream<Term> termStream = Arrays.stream(queryTerms).map(t -> new Term(field, t));
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
		return dfSum / queryTerms.length;
	}

	protected double minNormalizedDocumentFrequency(IndexReader indexReader, String[] queryTerms, String field) {
		Stream<Term> termStream = Arrays.stream(queryTerms).map(t -> new Term(field, t));
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

	protected double coveredBiwordRatio(IndexSearcher indexSearcher, String query, String field) {
		int coveredBiwordCounts = 0;
		int biwordCount = 0;
		try (StandardAnalyzer analyzer = new StandardAnalyzer();
				TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			String prevTerm = null;
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();
				if (prevTerm != null) {
					biwordCount++;
					PhraseQuery.Builder builder = new PhraseQuery.Builder();
					builder.add(new Term(field, prevTerm), 0);
					builder.add(new Term(field, term), 1);
					PhraseQuery pq = builder.build();
					ScoreDoc[] hits = indexSearcher.search(pq, 10000).scoreDocs;
					if (hits.length > 0)
						coveredBiwordCounts++;
				}
				prevTerm = term;
			}
			tokenStream.end();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return coveredBiwordCounts / (double) biwordCount;
	}

	protected double meanNormalizedDocumentBiwordFrequency(IndexSearcher indexSearcher, String query, String field) {
		double normalizedBiwordDocFrequencySum = 0;
		int bigramCount = 0;
		try (StandardAnalyzer analyzer = new StandardAnalyzer();
				TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			String prevTerm = null;
			double prevDf = 0;
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();
				if (prevTerm != null) {
					bigramCount++;
					PhraseQuery.Builder builder = new PhraseQuery.Builder();
					builder.add(new Term(field, prevTerm), 0);
					builder.add(new Term(field, term), 1);
					PhraseQuery pq = builder.build();
					ScoreDoc[] hits = indexSearcher.search(pq, 10000).scoreDocs;
					normalizedBiwordDocFrequencySum += (hits.length / prevDf);
				}
				prevTerm = term;
				prevDf = indexSearcher.getIndexReader().docFreq(new Term(field, prevTerm));
			}
			tokenStream.end();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return normalizedBiwordDocFrequencySum / (double) bigramCount;
	}

	protected double minNormalizedDocumentBiwordFrequency(IndexSearcher indexSearcher, String query, String field) {
		double minNormalizedBiwordDocFrequency = 1;
		try (StandardAnalyzer analyzer = new StandardAnalyzer();
				TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			String prevTerm = null;
			double prevDf = 0;
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();
				if (prevTerm != null) {
					PhraseQuery.Builder builder = new PhraseQuery.Builder();
					builder.add(new Term(field, prevTerm), 0);
					builder.add(new Term(field, term), 1);
					PhraseQuery pq = builder.build();
					ScoreDoc[] hits = indexSearcher.search(pq, 10000).scoreDocs;
					double currentBiwordDocFrequency = hits.length / prevDf;
					minNormalizedBiwordDocFrequency = Math.min(currentBiwordDocFrequency,
							minNormalizedBiwordDocFrequency);
				}
				prevTerm = term;
				prevDf = indexSearcher.getIndexReader().docFreq(new Term(field, prevTerm));
			}
			tokenStream.end();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return minNormalizedBiwordDocFrequency;
	}

	protected double meanBM25Score(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
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

	protected double minBM25Score(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
		Query query = lqb.buildQuery(queryText);
		ScoreDoc[] scoreDocHits = null;
		int k = 20;
		try {
			scoreDocHits = searcher.search(query, k).scoreDocs;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (scoreDocHits != null)
			return Arrays.stream(scoreDocHits).map(h -> h.score).reduce(Float::min).orElse(0f);
		else
			return 0;
	}

	protected double meanBoolScore(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
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

	protected double minBoolScore(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
		Query query = lqb.buildQuery(queryText, Operator.AND);
		ScoreDoc[] scoreDocHits = null;
		int k = 20;
		try {
			scoreDocHits = searcher.search(query, k).scoreDocs;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (scoreDocHits != null)
			return Arrays.stream(scoreDocHits).map(h -> h.score).reduce(Float::min).orElse(0f);
		else
			return 0;
	}

	protected double meanAverageTermDocPopularity(IndexSearcher searcher, String queryText, String field) {
		double sum = 0;
		int tokenCounts = 0;
		try (StandardAnalyzer analyzer = new StandardAnalyzer();
				TokenStream tokenStream = analyzer.tokenStream(field,
						new StringReader(queryText.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				tokenCounts++;
				String term = termAtt.toString();
				TermQuery query = new TermQuery(new Term(field, term));
				// Query query = lqb.buildQuery(queryText);
				int k = 200;
				try {
					ScoreDoc[] hits = searcher.search(query, k).scoreDocs;
					sum += Arrays.stream(hits).map(h -> {
						try {
							return Double.parseDouble(searcher.doc(h.doc).get(WikiFileIndexer.WEIGHT_ATTRIB));
						} catch (IOException e) {
							LOGGER.log(Level.SEVERE, e.getMessage(), e);
							return 0.0;
						}
					}).reduce(Double::sum).orElse(0.0) / Math.max(1, Math.min(k, hits.length));
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (tokenCounts > 0)
			return sum / tokenCounts;
		else
			return 0;
	}

	protected double minAverageTermDocPopularity(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb,
			String field) {
		double min = -1;
		try (StandardAnalyzer analyzer = new StandardAnalyzer();
				TokenStream tokenStream = analyzer.tokenStream(field,
						new StringReader(queryText.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();
				TermQuery query = new TermQuery(new Term(field, term));
				// Query query = lqb.buildQuery(queryText);
				int k = 200;
				try {
					ScoreDoc[] hits = searcher.search(query, k).scoreDocs;
					double current = Arrays.stream(hits).map(h -> {
						try {
							return Double.parseDouble(searcher.doc(h.doc).get(WikiFileIndexer.WEIGHT_ATTRIB));
						} catch (IOException e) {
							LOGGER.log(Level.SEVERE, e.getMessage(), e);
							return 0.0;
						}
					}).reduce(Double::sum).orElse(0.0) / Math.max(1, Math.min(k, hits.length));
					if (min == -1)
						min = current;
					else
						min = (min < current) ? min : current;
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return min;
	}
}
