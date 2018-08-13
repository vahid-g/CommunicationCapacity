package irstyle;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import cache_selection_ml.FeatureExtraction;
import indexing.BiwordAnalyzer;
import indexing.popularity.TokenPopularity;
import query.ExperimentQuery;
import query.QueryServices;

public class RunFeatureExtractionForStructuredWiki {

	public static final Logger LOGGER = Logger.getLogger(RunFeatureExtractionForStructuredWiki.class.getName());

	public static void main(String[] args) throws IOException {
		// inputs
		// popularity indices
		// query log
		List<String> argList = Arrays.asList(args);
		List<ExperimentQuery> queries;
		if (argList.contains("-inex")) {
			queries = QueryServices.loadInexQueries();
		} else {
			queries = QueryServices.loadMsnQueries();
		}

		String baseDataDir = "/data/ghadakcv/wikipedia/";
		Path cacheIndexPath = Paths.get(baseDataDir + "lm_cache_mrr");
		Path restIndexPath = Paths.get(baseDataDir + "lm_rest_mrr");
		Path allIndexPath = Paths.get(baseDataDir + "lm_all_mrr");
		Path biwordIndexPath = Paths.get("lm_cache_mrr_bi");
		Path biwordRestIndexPath = Paths.get("lm_rest_mrr_bi");
		Path globalBiwordIndexPath = Paths.get("lm_all_mrr_bi");

		FeatureExtraction wqde = new FeatureExtraction(RelationalWikiIndexer.WEIGHT_FIELD);
		LOGGER.log(Level.INFO, "loading popularity indices..");
		//TODO
		Map<String, TokenPopularity> termPopularity = TokenPopularity
				.loadTokenPopularities(cacheIndexPath + "_title_pop_fast_tokens" + ".csv");
		Map<String, TokenPopularity> biwordPopularity = TokenPopularity
				.loadTokenPopularities(biwordIndexPath + "_title_pop_fast_tokens" + ".csv");
		LOGGER.log(Level.INFO, "loading done!");
		List<String> data = new ArrayList<String>();
		String[] featureNames = { "query", "covered", "covered_rest", "mean_df", "mean_df_rest", "min_df",
				"min_df_rest", "mean_mean_pop", "mean_min_pop", "min_mean_pop", "min_min_pop", "ql", "ql_rest", "qll",
				"qll_rest", "covered_bi", "covered_bi_rest", "mean_df_bi", "mean_df_bi_rest", "min_df_bi",
				"min_df_bi_rest", "mean_mean_pop_bi", "mean_min_pop_bi", "min_mean_pop_bi", "min_min_pop_bi", "ql_bi",
				"ql_bi_rest", "qll_bi", "qll_bi_rest" };
		data.add(Arrays.asList(featureNames).stream().map(ft -> ft + ",").collect(Collectors.joining()));
		try (IndexReader indexReader = DirectoryReader.open(FSDirectory.open(cacheIndexPath));
				IndexReader restIndexReader = DirectoryReader.open(FSDirectory.open(restIndexPath));
				IndexReader globalIndexReader = DirectoryReader.open(FSDirectory.open(allIndexPath));
				IndexReader biwordIndexReader = DirectoryReader.open(FSDirectory.open(biwordIndexPath));
				IndexReader biwordRestIndexReader = DirectoryReader.open(FSDirectory.open(biwordRestIndexPath));
				IndexReader globalBiwordIndexReader = DirectoryReader.open(FSDirectory.open(globalBiwordIndexPath));
				Analyzer biwordAnalyzer = new BiwordAnalyzer();
				Analyzer analyzer = new StandardAnalyzer()) {
			long start = System.currentTimeMillis();
			for (ExperimentQuery query : queries) {
				String queryText = query.getText();
				List<Double> f = new ArrayList<Double>();
				f.add(wqde.coveredTokenRatio(indexReader, queryText, RelationalWikiIndexer.TEXT_FIELD, analyzer));
				f.add(wqde.coveredTokenRatio(restIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD, analyzer));
				f.add(wqde.meanNormalizedTokenDocumentFrequency(indexReader, queryText,
						RelationalWikiIndexer.TEXT_FIELD, analyzer));
				f.add(wqde.meanNormalizedTokenDocumentFrequency(restIndexReader, queryText,
						RelationalWikiIndexer.TEXT_FIELD, analyzer));
				f.add(wqde.minNormalizedTokenDocumentFrequency(indexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						analyzer));
				f.add(wqde.minNormalizedTokenDocumentFrequency(restIndexReader, queryText,
						RelationalWikiIndexer.TEXT_FIELD, analyzer));
				List<Double> averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(termPopularity, queryText,
						RelationalWikiIndexer.TEXT_FIELD, analyzer);
				f.addAll(averageTokenDocPopularity);
				f.add(wqde.queryLikelihood(indexReader, queryText, RelationalWikiIndexer.TEXT_FIELD, globalIndexReader,
						analyzer));
				f.add(wqde.queryLikelihood(restIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						globalIndexReader, analyzer));
				f.add(wqde.queryLogLikelihood(indexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						globalIndexReader, analyzer));
				f.add(wqde.queryLogLikelihood(restIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						globalIndexReader, analyzer));
				f.add(wqde.coveredTokenRatio(biwordIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						biwordAnalyzer));
				f.add(wqde.coveredTokenRatio(biwordRestIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						biwordAnalyzer));
				f.add(wqde.meanNormalizedTokenDocumentFrequency(biwordIndexReader, queryText,
						RelationalWikiIndexer.TEXT_FIELD, biwordAnalyzer));
				f.add(wqde.meanNormalizedTokenDocumentFrequency(biwordRestIndexReader, queryText,
						RelationalWikiIndexer.TEXT_FIELD, biwordAnalyzer));
				f.add(wqde.minNormalizedTokenDocumentFrequency(biwordIndexReader, queryText,
						RelationalWikiIndexer.TEXT_FIELD, biwordAnalyzer));
				f.add(wqde.minNormalizedTokenDocumentFrequency(biwordRestIndexReader, queryText,
						RelationalWikiIndexer.TEXT_FIELD, biwordAnalyzer));
				averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(biwordPopularity, queryText,
						RelationalWikiIndexer.TEXT_FIELD, biwordAnalyzer);
				f.addAll(averageTokenDocPopularity);
				f.add(wqde.queryLikelihood(biwordIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						globalBiwordIndexReader, biwordAnalyzer));
				f.add(wqde.queryLikelihood(biwordRestIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						globalBiwordIndexReader, biwordAnalyzer));
				f.add(wqde.queryLogLikelihood(biwordIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						globalBiwordIndexReader, biwordAnalyzer));
				f.add(wqde.queryLogLikelihood(biwordRestIndexReader, queryText, RelationalWikiIndexer.TEXT_FIELD,
						globalBiwordIndexReader, biwordAnalyzer));
				data.add(queryText + "," + f.stream().map(ft -> ft + ",").collect(Collectors.joining()));
			}
			long end = System.currentTimeMillis();
			double time = end - start;
			LOGGER.log(Level.INFO, "Time spent per query: " + time / queries.size() + " (ms)");
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

		try (FileWriter fw = new FileWriter("features.csv")) {
			for (String line : data) {
				fw.write(line + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

}
