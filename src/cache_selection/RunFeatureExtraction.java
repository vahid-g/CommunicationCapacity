package wiki13.cache_selection;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

import indexing.BiwordAnalyzer;
import indexing.popularity.TokenPopularity;
import query.ExperimentQuery;
import query.LuceneQueryBuilder;
import query.QueryServices;
import wiki13.WikiFileIndexer;
import wiki13.WikiFilesPaths;

public class RunFeatureExtraction {

	public static final Logger LOGGER = Logger.getLogger(RunFeatureExtraction.class.getName());

	public static void main(String[] args) throws IOException {

		Options options = new Options();
		Option expNumberOption = new Option("exp", true, "Number of experiment");
		expNumberOption.setRequired(true);
		options.addOption(expNumberOption);
		Option totalExpNumberOption = new Option("total", true, "Total number of experiments");
		totalExpNumberOption.setRequired(true);
		options.addOption(totalExpNumberOption);
		Option querysetOption = new Option("queryset", true, "specifies the query log (msn/inex)");
		querysetOption.setRequired(true);
		options.addOption(querysetOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			cl = clp.parse(options, args);
			WikiFilesPaths paths = WikiFilesPaths.getMaplePaths();
			String exp = cl.getOptionValue("exp");
			String totalExp = cl.getOptionValue("total");
			Path indexPath = Paths.get(paths.getIndexBase() + exp);
			Path globalIndexPath = Paths.get(paths.getIndexBase() + totalExp);
			Path biwordIndexPath = Paths.get(paths.getBiwordIndexBase() + exp);
			Path globalBiwordIndexPath = Paths.get(paths.getBiwordIndexBase() + totalExp);
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
			RunFeatureExtraction wqde = new RunFeatureExtraction();
			LOGGER.log(Level.INFO, "loading popularity indices..");
			Map<String, TokenPopularity> termTitlePopularity = TokenPopularity
					.loadTokenPopularities(indexPath + "_title_pop_fast" + ".csv");
			Map<String, TokenPopularity> termContentPopularity = TokenPopularity
					.loadTokenPopularities(indexPath + "_content_pop_fast" + ".csv");
			Map<String, TokenPopularity> biwordTitlePopularity = TokenPopularity
					.loadTokenPopularities(biwordIndexPath + "_title_pop_fast" + ".csv");
			Map<String, TokenPopularity> biwordContentPopularity = TokenPopularity
					.loadTokenPopularities(biwordIndexPath + "_content_pop_fast" + ".csv");
			LOGGER.log(Level.INFO, "loading done!");
			List<String> data = new ArrayList<String>();
			String[] featureNames = { "query", "covered_t", "covered_c", "mean_df_t", "mean_df_c", "min_df_t",
					"min_df_c", "mean_mean_pop_t", "mean_min_pop_t", "min_mean_pop_t", "min_min_pop_t",
					"mean_mean_pop_c", "mean_min_pop_c", "min_mean_pop_c", "min_min_pop_c", "ql_t", "ql_c", "qll_t",
					"qll_c", "covered_t_bi", "covered_c_bi", "mean_df_t_bi", "mean_df_c_bi", "min_df_t_bi",
					"min_df_c_bi", "mean_mean_pop_t_bi", "mean_min_pop_t_bi", "min_mean_pop_t_bi", "min_min_pop_t_bi",
					"mean_mean_pop_c_bi", "mean_min_pop_c_bi", "min_mean_pop_c_bi", "min_min_pop_c_bi", "ql_t_bi",
					"ql_c_bi", "qll_t_bi", "qll_c_bi" };
			data.add(Arrays.asList(featureNames).stream().map(ft -> ft + ",").collect(Collectors.joining()));
			try (IndexReader indexReader = DirectoryReader.open(FSDirectory.open(indexPath));
					IndexReader globalIndexReader = DirectoryReader.open(FSDirectory.open(globalIndexPath));
					IndexReader biwordIndexReader = DirectoryReader.open(FSDirectory.open(biwordIndexPath));
					IndexReader globalBiwordIndexReader = DirectoryReader.open(FSDirectory.open(globalBiwordIndexPath));
					Analyzer biwordAnalyzer = new BiwordAnalyzer();
					Analyzer analyzer = new StandardAnalyzer()) {
				for (ExperimentQuery query : queries) {
					String queryText = query.getText();
					List<Double> f = new ArrayList<Double>();
					f.add(wqde.coveredTokenRatio(indexReader, queryText, WikiFileIndexer.TITLE_ATTRIB, analyzer));
					f.add(wqde.coveredTokenRatio(indexReader, queryText, WikiFileIndexer.CONTENT_ATTRIB, analyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(indexReader, queryText,
							WikiFileIndexer.TITLE_ATTRIB, analyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(indexReader, queryText,
							WikiFileIndexer.CONTENT_ATTRIB, analyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(indexReader, queryText, WikiFileIndexer.TITLE_ATTRIB,
							analyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(indexReader, queryText,
							WikiFileIndexer.CONTENT_ATTRIB, analyzer));
					List<Double> averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(termTitlePopularity,
							queryText, WikiFileIndexer.TITLE_ATTRIB, analyzer);
					// wqde.tokenPopularityFeatures(indexReader, queryText,
					// WikiFileIndexer.TITLE_ATTRIB, analyzer);
					f.addAll(averageTokenDocPopularity);
					averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(termContentPopularity, queryText,
							WikiFileIndexer.CONTENT_ATTRIB, analyzer);
					f.addAll(averageTokenDocPopularity);
					f.add(wqde.queryLikelihood(indexReader, queryText, WikiFileIndexer.TITLE_ATTRIB, globalIndexReader,
							analyzer));
					f.add(wqde.queryLikelihood(indexReader, queryText, WikiFileIndexer.CONTENT_ATTRIB,
							globalIndexReader, analyzer));
					f.add(wqde.queryLogLikelihood(indexReader, queryText, WikiFileIndexer.TITLE_ATTRIB,
							globalIndexReader, analyzer));
					f.add(wqde.queryLogLikelihood(indexReader, queryText, WikiFileIndexer.CONTENT_ATTRIB,
							globalIndexReader, analyzer));
					f.add(wqde.coveredTokenRatio(biwordIndexReader, queryText, WikiFileIndexer.TITLE_ATTRIB,
							biwordAnalyzer));
					f.add(wqde.coveredTokenRatio(biwordIndexReader, queryText, WikiFileIndexer.CONTENT_ATTRIB,
							biwordAnalyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(biwordIndexReader, queryText,
							WikiFileIndexer.TITLE_ATTRIB, biwordAnalyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(biwordIndexReader, queryText,
							WikiFileIndexer.CONTENT_ATTRIB, biwordAnalyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(biwordIndexReader, queryText,
							WikiFileIndexer.TITLE_ATTRIB, biwordAnalyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(biwordIndexReader, queryText,
							WikiFileIndexer.CONTENT_ATTRIB, biwordAnalyzer));
					averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(biwordTitlePopularity, queryText,
							WikiFileIndexer.TITLE_ATTRIB, biwordAnalyzer);
					f.addAll(averageTokenDocPopularity);
					averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(biwordContentPopularity, queryText,
							WikiFileIndexer.CONTENT_ATTRIB, biwordAnalyzer);
					f.addAll(averageTokenDocPopularity);
					f.add(wqde.queryLikelihood(biwordIndexReader, queryText, WikiFileIndexer.TITLE_ATTRIB,
							globalBiwordIndexReader, biwordAnalyzer));
					f.add(wqde.queryLikelihood(biwordIndexReader, queryText, WikiFileIndexer.CONTENT_ATTRIB,
							globalBiwordIndexReader, biwordAnalyzer));
					f.add(wqde.queryLogLikelihood(biwordIndexReader, queryText, WikiFileIndexer.TITLE_ATTRIB,
							globalBiwordIndexReader, biwordAnalyzer));
					f.add(wqde.queryLogLikelihood(biwordIndexReader, queryText, WikiFileIndexer.CONTENT_ATTRIB,
							globalBiwordIndexReader, biwordAnalyzer));
					data.add(queryText + "," + f.stream().map(ft -> ft + ",").collect(Collectors.joining()));
				}
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}

			try (FileWriter fw = new FileWriter(queryset + "_" + exp + ".csv")) {
				for (String line : data) {
					fw.write(line + "\n");
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

	protected double coveredTokenRatio(IndexReader indexReader, String query, String field, Analyzer analyzer) {
		int coveredBiwordCounts = 0;
		int biwordCount = 0;
		try (TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				biwordCount++;
				String biword = termAtt.toString();
				if (indexReader.docFreq(new Term(field, biword)) > 0) {
					coveredBiwordCounts++;
				}
			}
			tokenStream.end();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (biwordCount == 0)
			return 0;
		return coveredBiwordCounts / (double) biwordCount;
	}

	protected double meanNormalizedTokenDocumentFrequency(IndexReader indexReader, String query, String field,
			Analyzer analyzer) {
		double normalizedBiwordDocFrequencySum = 0;
		int tokenCount = 0;
		double N = 1.0;
		try (TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();
				normalizedBiwordDocFrequencySum += indexReader.docFreq(new Term(field, term));
				tokenCount++;
			}
			tokenStream.end();
			N = indexReader.getDocCount(field);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (tokenCount == 0)
			return 0;
		return normalizedBiwordDocFrequencySum / (N * tokenCount);
	}

	protected double minNormalizedTokenDocumentFrequency(IndexReader indexReader, String query, String field,
			Analyzer analyzer) {
		double minNormalizedTokenDocFrequency = Double.MAX_VALUE;
		double N = 1.0;
		try (TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();
				minNormalizedTokenDocFrequency = Math.min(minNormalizedTokenDocFrequency,
						indexReader.docFreq(new Term(field, term)));
			}
			tokenStream.end();
			N = indexReader.getDocCount(field);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return minNormalizedTokenDocFrequency / N;
	}

	protected List<Double> tokenPopularityFeatures(IndexReader indexReader, String query, String field,
			Analyzer analyzer) {
		double averagePopularitySum = 0;
		double minAverage = Double.MAX_VALUE;
		double minPopularitySum = 0;
		double minMin = Double.MAX_VALUE;
		int biwordCount = 0;
		try (TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				biwordCount++;
				double biwordPopularitySum = 0;
				double biwordMinPopularity = Double.MAX_VALUE;
				double postingSize = 0;
				for (LeafReaderContext lrc : indexReader.leaves()) {
					LeafReader lr = lrc.reader();
					PostingsEnum pe = lr.postings(new Term(field, termAtt.toString()));
					if (pe == null) {
						continue;
					}
					int docId = pe.nextDoc();
					while (docId != PostingsEnum.NO_MORE_DOCS) {
						Document doc = lr.document(docId);
						double termDocPopularity = Double.parseDouble(doc.get(WikiFileIndexer.WEIGHT_ATTRIB));
						biwordPopularitySum += termDocPopularity;
						postingSize++;
						biwordMinPopularity = Math.min(biwordMinPopularity, termDocPopularity);
						docId = pe.nextDoc();
					}
				}
				double termPopularityAverage = biwordPopularitySum / Math.max(postingSize, 1);
				averagePopularitySum += termPopularityAverage;
				minAverage = Math.min(minAverage, termPopularityAverage);
				minPopularitySum += biwordMinPopularity;
				minMin = Math.min(minMin, biwordMinPopularity);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		double meanAverage = 0;
		double meanMin = 0;
		if (biwordCount > 0) {
			meanAverage = averagePopularitySum / biwordCount;
			meanMin = minPopularitySum / biwordCount;
		}
		if (minAverage == Double.MAX_VALUE) {
			minAverage = 0;
		}
		if (minMin == Double.MAX_VALUE) {
			minMin = 0;
		}
		List<Double> result = new ArrayList<Double>();
		result.add(meanAverage);
		result.add(meanMin);
		result.add(minAverage);
		result.add(minMin);
		return result;
	}

	protected List<Double> fastTokenPopularityFeatures(Map<String, TokenPopularity> popularityMap, String query,
			String field, Analyzer analyzer) {
		double averagePopularitySum = 0;
		double minAverage = Double.MAX_VALUE;
		double minPopularitySum = 0;
		double minMin = Double.MAX_VALUE;
		int tokenCount = 0;
		try (TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				String token = termAtt.toString();
				tokenCount++;
				if (!popularityMap.containsKey(token)) {
					continue;
				}
				double tokenMinPopularity = popularityMap.get(token).min;
				double tokenPopularityAverage = popularityMap.get(token).mean;
				averagePopularitySum += tokenPopularityAverage;
				minAverage = Math.min(minAverage, tokenPopularityAverage);
				minPopularitySum += tokenMinPopularity;
				minMin = Math.min(minMin, tokenMinPopularity);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		double meanAverage = 0;
		double meanMin = 0;
		if (tokenCount > 0) {
			meanAverage = averagePopularitySum / tokenCount;
			meanMin = minPopularitySum / tokenCount;
		}
		if (minAverage == Double.MAX_VALUE) {
			minAverage = 0;
		}
		if (minMin == Double.MAX_VALUE) {
			minMin = 0;
		}
		List<Double> result = new ArrayList<Double>();
		result.add(meanAverage);
		result.add(meanMin);
		result.add(minAverage);
		result.add(minMin);
		return result;
	}

	protected double queryLikelihood(IndexReader reader, String query, String field, IndexReader globalIndexReader,
			Analyzer analyzer) throws IOException {
		long tfSum = reader.getSumTotalTermFreq(field);
		long globalTfSum = globalIndexReader.getSumTotalTermFreq(field);
		double likelihood = 0;
		double p = 1.0;
		try (TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();
				Term currentTokenTerm = new Term(field, term);
				double tf = reader.totalTermFreq(currentTokenTerm);
				double gtf = globalIndexReader.totalTermFreq(currentTokenTerm);
				double probabilityOfTermGivenSubset = tf / tfSum;
				double probabilityOfTermGivenDatabase = gtf / globalTfSum;
				p *= (0.9 * probabilityOfTermGivenSubset + 0.1 * probabilityOfTermGivenDatabase);
			}
			tokenStream.end();
			likelihood = p;
		}
		return likelihood;
	}

	protected double queryLogLikelihood(IndexReader reader, String query, String field, IndexReader globalIndexReader,
			Analyzer analyzer) throws IOException {
		long tfSum = reader.getSumTotalTermFreq(field);
		long globalTfSum = globalIndexReader.getSumTotalTermFreq(field);
		double likelihood = 0;
		double p = 0;
		try (TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();
				Term currentTokenTerm = new Term(field, term);
				double tf = reader.totalTermFreq(currentTokenTerm);
				double gtf = globalIndexReader.totalTermFreq(currentTokenTerm);
				double probabilityOfTermGivenSubset = tf / tfSum;
				double probabilityOfTermGivenDatabase = gtf / globalTfSum;
				p = 0.9 * probabilityOfTermGivenSubset + 0.1 * probabilityOfTermGivenDatabase;
				if (p > 0) {
					likelihood += Math.log(p);
				}
			}
			tokenStream.end();
		}
		return likelihood;
	}

	@Deprecated
	protected double meanBM25Score(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
		Query query = lqb.buildQuery(queryText);
		ScoreDoc[] scoreDocHits = null;
		int k = 200;
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

	@Deprecated
	protected double minBM25Score(IndexSearcher searcher, String queryText, LuceneQueryBuilder lqb) {
		Query query = lqb.buildQuery(queryText);
		ScoreDoc[] scoreDocHits = null;
		int k = 200;
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

	@Deprecated
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

	@Deprecated
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

	@Deprecated
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

	@Deprecated
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

	@Deprecated
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

	@Deprecated
	protected List<Double> termPopularityFeatures(IndexReader reader, String queryText, String field) {
		double averagePopularitySum = 0;
		double minAverage = Double.MAX_VALUE;
		double minPopularitySum = 0;
		double minMin = Double.MAX_VALUE;
		int tokenCounts = 0;
		List<Double> result = new ArrayList<Double>();
		try (StandardAnalyzer analyzer = new StandardAnalyzer();
				TokenStream tokenStream = analyzer.tokenStream(field,
						new StringReader(queryText.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				tokenCounts++;
				double termPopularitySum = 0;
				double termMinPopularity = Double.MAX_VALUE;
				double postingSize = 0;
				for (LeafReaderContext lrc : reader.leaves()) {
					LeafReader lr = lrc.reader();
					PostingsEnum pe = lr.postings(new Term(field, termAtt.toString()));
					int docId = pe.nextDoc();
					while (docId != PostingsEnum.NO_MORE_DOCS) {
						Document doc = lr.document(docId);
						double termDocPopularity = Double.parseDouble(doc.get(WikiFileIndexer.WEIGHT_ATTRIB));
						termPopularitySum += termDocPopularity;
						postingSize++;
						termMinPopularity = Math.min(termMinPopularity, termDocPopularity);
						docId = pe.nextDoc();
					}
				}
				double termPopularityAverage = termPopularitySum / Math.max(postingSize, 1);
				averagePopularitySum += termPopularityAverage;
				minAverage = Math.min(minAverage, termPopularityAverage);
				minPopularitySum += termMinPopularity;
				minMin = Math.min(minMin, termMinPopularity);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		double meanAverage = 0;
		double meanMin = 0;
		if (tokenCounts > 0) {
			meanAverage = averagePopularitySum / tokenCounts;
			meanMin = minPopularitySum / tokenCounts;
		}
		if (minAverage == Double.MAX_VALUE) {
			minAverage = 0;
		}
		if (minMin == Double.MAX_VALUE) {
			minMin = 0;
		}
		result.add(meanAverage);
		result.add(meanMin);
		result.add(minAverage);
		result.add(minMin);
		return result;
	}

	@Deprecated
	protected double queryLikelihood(IndexReader reader, String query, String field, IndexReader globalIndexReader)
			throws IOException {
		long tfSum = reader.getSumTotalTermFreq(field);
		long globalTfSum = globalIndexReader.getSumTotalTermFreq(field);
		double likelihood = 0;
		try (Analyzer analyzer = new StandardAnalyzer()) {
			TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(query.replaceAll("'", "`")));
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			double p = 1.0;
			try {
				tokenStream.reset();
				while (tokenStream.incrementToken()) {
					String term = termAtt.toString();
					Term currentTokenTerm = new Term(field, term);
					double tf = reader.totalTermFreq(currentTokenTerm);
					double gtf = globalIndexReader.totalTermFreq(currentTokenTerm);
					if (gtf == 0) {
						LOGGER.log(Level.WARNING, "zero gtf for: " + term);
					}
					double probabilityOfTermGivenSubset = tf / tfSum;
					double probabilityOfTermGivenDatabase = gtf / globalTfSum;
					p *= (0.9 * probabilityOfTermGivenSubset + 0.1 * probabilityOfTermGivenDatabase);
				}
				tokenStream.end();
				likelihood = p;
			} finally {
				tokenStream.close();
			}
		}
		return likelihood;
	}

	@Deprecated
	protected double coveredBiwordRatio(IndexSearcher indexSearcher, String query, String field) {
		int coveredBiwordCounts = 0;
		int biwordCount = 0;
		int k = 20;
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
					ScoreDoc[] hits = indexSearcher.search(pq, k).scoreDocs;
					if (hits.length > 0)
						coveredBiwordCounts++;
				}
				prevTerm = term;
			}
			tokenStream.end();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (biwordCount == 0)
			return 0;
		return coveredBiwordCounts / (double) biwordCount;
	}

	@Deprecated
	protected double meanNormalizedDocumentBiwordFrequency(IndexSearcher indexSearcher, String query, String field) {
		double normalizedBiwordDocFrequencySum = 0;
		int bigramCount = 0;
		int k = 10000;
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
					ScoreDoc[] hits = indexSearcher.search(pq, k).scoreDocs;
					if (hits.length != 0)
						normalizedBiwordDocFrequencySum += (hits.length / prevDf);
				}
				prevTerm = term;
				prevDf = indexSearcher.getIndexReader().docFreq(new Term(field, prevTerm));
			}
			tokenStream.end();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (bigramCount == 0)
			return 0;
		return normalizedBiwordDocFrequencySum / (double) bigramCount;
	}

	@Deprecated
	protected double minNormalizedDocumentBiwordFrequency(IndexSearcher indexSearcher, String query, String field) {
		double minNormalizedBiwordDocFrequency = 1;
		int k = 10000;
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
					ScoreDoc[] hits = indexSearcher.search(pq, k).scoreDocs;
					if (hits.length == 0) {
						minNormalizedBiwordDocFrequency = 0;
					} else {
						double currentBiwordDocFrequency = hits.length / prevDf;
						minNormalizedBiwordDocFrequency = Math.min(currentBiwordDocFrequency,
								minNormalizedBiwordDocFrequency);
					}
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

	@Deprecated
	protected List<Double> termPopularityFeatures(IndexSearcher searcher, String queryText, String field) {
		double meanAverage = 0;
		double minAverage = -1;
		double meanMin = 0;
		double minMin = -1;
		int tokenCounts = 0;
		int k = 10000;
		List<Double> result = new ArrayList<Double>();
		try (StandardAnalyzer analyzer = new StandardAnalyzer();
				TokenStream tokenStream = analyzer.tokenStream(field,
						new StringReader(queryText.replaceAll("'", "`")))) {
			CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				tokenCounts++;
				String term = termAtt.toString();
				TermQuery query = new TermQuery(new Term(field, term));
				try {
					ScoreDoc[] hits = searcher.search(query, k).scoreDocs;
					List<Double> weights = Arrays.stream(hits).map(h -> {
						try {
							return Double.parseDouble(searcher.doc(h.doc).get(WikiFileIndexer.WEIGHT_ATTRIB));
						} catch (IOException e) {
							LOGGER.log(Level.SEVERE, e.getMessage(), e);
							return 0.0;
						}
					}).collect(Collectors.toList());
					double currentTermAverage = weights.stream().reduce(Double::sum).orElse(0.0)
							/ Math.max(1, Math.min(k, weights.size()));
					meanAverage += currentTermAverage;
					if (minAverage == -1) {
						minAverage = currentTermAverage;
					} else if (currentTermAverage < minAverage) {
						minAverage = currentTermAverage;
					}
					double currentTermMin = weights.stream().reduce(Double::min).orElse(0.0);
					meanMin += currentTermMin;
					if (minMin == -1) {
						minMin = currentTermMin;
					} else if (currentTermMin < minMin) {
						minMin = currentTermMin;
					}
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		if (tokenCounts > 0) {
			meanAverage = meanAverage / tokenCounts;
			meanMin = meanMin / tokenCounts;
		}
		result.add(meanAverage);
		result.add(meanMin);
		result.add(minAverage);
		result.add(minMin);
		return result;
	}

	@Deprecated
	protected List<Double> biwordPopularityFeatures(IndexSearcher indexSearcher, String query, String field) {
		double meanAverage = 0;
		double minAverage = -1;
		double meanMin = 0;
		double minMin = -1;
		int biwordCount = 0;
		int k = 10000;
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
					ScoreDoc[] hits = indexSearcher.search(pq, k).scoreDocs;
					List<Double> weights = Arrays.stream(hits).map(h -> {
						try {
							return Double.parseDouble(indexSearcher.doc(h.doc).get(WikiFileIndexer.WEIGHT_ATTRIB));
						} catch (IOException e) {
							LOGGER.log(Level.SEVERE, e.getMessage(), e);
							return 0.0;
						}
					}).collect(Collectors.toList());
					double currentTermAverage = weights.stream().reduce(Double::sum).orElse(0.0)
							/ Math.max(1, Math.min(k, weights.size()));
					meanAverage += currentTermAverage;
					if (minAverage == -1) {
						minAverage = currentTermAverage;
					} else if (currentTermAverage < minAverage) {
						minAverage = currentTermAverage;
					}
					double currentTermMin = weights.stream().reduce(Double::min).orElse(0.0);
					meanMin += currentTermMin;
					if (minMin == -1) {
						minMin = currentTermMin;
					} else if (currentTermMin < minMin) {
						minMin = currentTermMin;
					}
				}
				prevTerm = term;
			}
			tokenStream.end();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		List<Double> result = new ArrayList<Double>();
		if (biwordCount > 0) {
			meanAverage = meanAverage / biwordCount;
			meanMin = meanMin / biwordCount;
		}
		result.add(meanAverage);
		result.add(meanMin);
		result.add(minAverage);
		result.add(minMin);
		return result;
	}
}
