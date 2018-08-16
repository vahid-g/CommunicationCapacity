package irstyle;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;

import cache_selection_ml.FeatureExtraction;
import indexing.BiwordAnalyzer;
import indexing.popularity.TokenPopularity;
import query.ExperimentQuery;
import query.QueryServices;

public class RunFeatureExtractionForMultiTables {

	public static final Logger LOGGER = Logger.getLogger(RunFeatureExtractionForMultiTables.class.getName());

	public static void main(String[] args) throws IOException {
		List<String> argList = Arrays.asList(args);
		List<ExperimentQuery> queries;
		queries = QueryServices.loadMsnQueriesAll();
		Collections.shuffle(queries, new Random(1));

		List<IndexReader> cacheIndexReader = new ArrayList<IndexReader>();
		List<IndexReader> restIndexReader = new ArrayList<IndexReader>();
		for (String table : ExperimentConstants.tableName) {
			for (String mode : new String[] { "_mrr", "_mrr_bi" }) {
				String indexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + table + "_cache_" + mode;
				cacheIndexReader.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
			}
		}

		FeatureExtraction wqde = new FeatureExtraction(RelationalWikiIndexer.WEIGHT_FIELD);
		LOGGER.log(Level.INFO, "loading popularity indices..");
		Map<String, TokenPopularity> cacheTermPopularity = TokenPopularity
				.loadTokenPopularities(ExperimentConstants.baseDataDir + "lm_cache_mrr" + "_text_pop_index" + ".csv");
		Map<String, TokenPopularity> restTermPopularity = TokenPopularity
				.loadTokenPopularities(ExperimentConstants.baseDataDir + "lm_rest_mrr" + "_text_pop_index" + ".csv");
		Map<String, TokenPopularity> biwordCachePopularity = TokenPopularity
				.loadTokenPopularities(ExperimentConstants.baseDataDir + "lm_cache_mrr_bi" + "_text_pop_index" + ".csv");
		Map<String, TokenPopularity> biwordRestPopularity = TokenPopularity
				.loadTokenPopularities(ExperimentConstants.baseDataDir + "lm_rest_mrr_bi" + "_text_pop_index" + ".csv");
		LOGGER.log(Level.INFO, "loading done!");
		List<String> data = new ArrayList<String>();
		String[] featureNames = { "query", "freq", "covered", "covered_rest", "mean_df", "mean_df_rest", "min_df",
				"min_df_rest", "mean_mean_pop", "mean_min_pop", "min_mean_pop", "min_min_pop", "mean_mean_pop_rest",
				"mean_min_pop_rest", "min_mean_pop_rest", "min_min_pop_rest", "ql", "ql_rest", "qll", "qll_rest",
				"covered_bi", "covered_bi_rest", "mean_df_bi", "mean_df_bi_rest", "min_df_bi", "min_df_bi_rest",
				"mean_mean_pop_bi", "mean_min_pop_bi", "min_mean_pop_bi", "min_min_pop_bi", "mean_mean_pop_bi_rest",
				"mean_min_pop_bi_rest", "min_mean_pop_bi_rest", "min_min_pop_bi_rest", "ql_bi", "ql_bi_rest", "qll_bi",
				"qll_bi_rest" };
		data.add(Arrays.asList(featureNames).stream().map(ft -> ft + ",").collect(Collectors.joining()));
		Analyzer biwordAnalyzer = new BiwordAnalyzer();
		Analyzer analyzer = new StandardAnalyzer();
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
				List<Double> averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(cacheTermPopularity,
						queryText, RelationalWikiIndexer.TEXT_FIELD, analyzer);
				f.addAll(averageTokenDocPopularity);
				averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(restTermPopularity, queryText,
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
				averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(biwordCachePopularity, queryText,
						RelationalWikiIndexer.TEXT_FIELD, biwordAnalyzer);
				f.addAll(averageTokenDocPopularity);
				averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(biwordRestPopularity, queryText,
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
				data.add(queryText + "," + query.getFreq() + ","
						+ f.stream().map(ft -> ft + ",").collect(Collectors.joining()));
			}
			long end = System.currentTimeMillis();
			double time = end - start;
			LOGGER.log(Level.INFO, "Time spent per query: " + time / queries.size() + " (ms)");
		}catch(

	IOException e)
	{
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

	try(
	FileWriter fw = new FileWriter("features.csv"))
	{
		for (String line : data) {
			fw.write(line + "\n");
		}
	}catch(
	IOException e)
	{
		LOGGER.log(Level.SEVERE, e.getMessage(), e);
	}
}

}
