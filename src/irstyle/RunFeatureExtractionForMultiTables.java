package irstyle;

import java.io.FileWriter;
import java.io.IOException;
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
import org.apache.lucene.store.FSDirectory;

import cache_selection_ml.FeatureExtraction;
import indexing.BiwordAnalyzer;
import indexing.popularity.TokenPopularity;
import query.ExperimentQuery;
import query.QueryServices;

public class RunFeatureExtractionForMultiTables {

	public static final Logger LOGGER = Logger.getLogger(RunFeatureExtractionForMultiTables.class.getName());

	public static void main(String[] args) throws IOException {
		// TODO: remove popularity features are duplicated
		List<ExperimentQuery> queries;
		queries = QueryServices.loadMsnQueriesAll();
		Collections.shuffle(queries, new Random(1));

		List<IndexReader> cacheIndexReaderList = new ArrayList<IndexReader>();
		List<IndexReader> restIndexReaderList = new ArrayList<IndexReader>();
		List<IndexReader> biCacheIndexReaderList = new ArrayList<IndexReader>();
		List<IndexReader> biRestIndexReaderList = new ArrayList<IndexReader>();
		for (String table : ExperimentConstants.tableName) {
			String indexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + table + "_cache_mrr";
			cacheIndexReaderList.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
			indexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + table + "_rest_mrr";
			restIndexReaderList.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
			indexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + table + "_cache_mrr_bi";
			biCacheIndexReaderList.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
			indexPath = RelationalWikiIndexer.DATA_WIKIPEDIA + "ml_" + table + "_rest_mrr_bi";
			biRestIndexReaderList.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
		}

		FeatureExtraction wqde = new FeatureExtraction(RelationalWikiIndexer.WEIGHT_FIELD);
		LOGGER.log(Level.INFO, "loading popularity indices..");
		Map<String, TokenPopularity> cacheTermPopularity = TokenPopularity
				.loadTokenPopularities(ExperimentConstants.baseDataDir + "lm_cache_mrr" + "_text_pop_index" + ".csv");
		Map<String, TokenPopularity> restTermPopularity = TokenPopularity
				.loadTokenPopularities(ExperimentConstants.baseDataDir + "lm_rest_mrr" + "_text_pop_index" + ".csv");
		Map<String, TokenPopularity> biwordCachePopularity = TokenPopularity.loadTokenPopularities(
				ExperimentConstants.baseDataDir + "lm_cache_mrr_bi" + "_text_pop_index" + ".csv");
		Map<String, TokenPopularity> biwordRestPopularity = TokenPopularity
				.loadTokenPopularities(ExperimentConstants.baseDataDir + "lm_rest_mrr_bi" + "_text_pop_index" + ".csv");
		LOGGER.log(Level.INFO, "loading done!");
		List<String> data = new ArrayList<String>();
		String[] featureNames = { "covered", "covered_rest", "mean_df", "mean_df_rest", "min_df", "min_df_rest",
				"mean_mean_pop", "mean_min_pop", "min_mean_pop", "min_min_pop", "mean_mean_pop_rest",
				"mean_min_pop_rest", "min_mean_pop_rest", "min_min_pop_rest", "ql", "ql_rest", "qll", "qll_rest",
				"covered_bi", "covered_bi_rest", "mean_df_bi", "mean_df_bi_rest", "min_df_bi", "min_df_bi_rest",
				"mean_mean_pop_bi", "mean_min_pop_bi", "min_mean_pop_bi", "min_min_pop_bi", "mean_mean_pop_bi_rest",
				"mean_min_pop_bi_rest", "min_mean_pop_bi_rest", "min_min_pop_bi_rest", "ql_bi", "ql_bi_rest", "qll_bi",
				"qll_bi_rest" };
		StringBuilder fileHeader = new StringBuilder();
		for (int i = 0; i < cacheIndexReaderList.size(); i++) {
			for (int j = 0; j < ExperimentConstants.textAttribs.length; j++) {
				final String suffix = i + "_" + j;
				fileHeader.append(Arrays.asList(featureNames).stream().map(ft -> ft + "_" + suffix + ",")
						.collect(Collectors.joining()));
			}
		}
		data.add("query, freq," + fileHeader);
		Analyzer biwordAnalyzer = new BiwordAnalyzer();
		Analyzer analyzer = new StandardAnalyzer();
		for (ExperimentQuery query : queries) {
			String queryText = query.getText();
			List<Double> f = new ArrayList<Double>();
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < cacheIndexReaderList.size(); i++) {
				IndexReader indexReader = cacheIndexReaderList.get(i);
				IndexReader restIndexReader = restIndexReaderList.get(i);
				IndexReader biwordIndexReader = biCacheIndexReaderList.get(i);
				IndexReader biwordRestIndexReader = biRestIndexReaderList.get(i);
				for (String attrib : ExperimentConstants.textAttribs[i]) {
					f.add(wqde.coveredTokenRatio(indexReader, queryText, attrib, analyzer));
					f.add(wqde.coveredTokenRatio(restIndexReader, queryText, attrib, analyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(indexReader, queryText, attrib, analyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(restIndexReader, queryText, attrib, analyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(indexReader, queryText, attrib, analyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(restIndexReader, queryText, attrib, analyzer));
					List<Double> averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(cacheTermPopularity,
							queryText, attrib, analyzer);
					f.addAll(averageTokenDocPopularity);
					averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(restTermPopularity, queryText, attrib,
							analyzer);
					f.addAll(averageTokenDocPopularity);
					f.add(wqde.queryLikelihood(indexReader, queryText, attrib, restIndexReader, analyzer));
					f.add(wqde.queryLikelihood(restIndexReader, queryText, attrib, indexReader, analyzer));
					f.add(wqde.queryLogLikelihood(indexReader, queryText, attrib, restIndexReader, analyzer));
					f.add(wqde.queryLogLikelihood(restIndexReader, queryText, attrib, indexReader, analyzer));
					f.add(wqde.coveredTokenRatio(biwordIndexReader, queryText, attrib, biwordAnalyzer));
					f.add(wqde.coveredTokenRatio(biwordRestIndexReader, queryText, attrib, biwordAnalyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(biwordIndexReader, queryText, attrib,
							biwordAnalyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(biwordRestIndexReader, queryText, attrib,
							biwordAnalyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(biwordIndexReader, queryText, attrib,
							biwordAnalyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(biwordRestIndexReader, queryText, attrib,
							biwordAnalyzer));
					averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(biwordCachePopularity, queryText,
							attrib, biwordAnalyzer);
					f.addAll(averageTokenDocPopularity);
					averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(biwordRestPopularity, queryText,
							attrib, biwordAnalyzer);
					f.addAll(averageTokenDocPopularity);
					f.add(wqde.queryLikelihood(biwordIndexReader, queryText, attrib, biwordRestIndexReader,
							biwordAnalyzer));
					f.add(wqde.queryLikelihood(biwordRestIndexReader, queryText, attrib, biwordIndexReader,
							biwordAnalyzer));
					f.add(wqde.queryLogLikelihood(biwordIndexReader, queryText, attrib, biwordRestIndexReader,
							biwordAnalyzer));
					f.add(wqde.queryLogLikelihood(biwordRestIndexReader, queryText, attrib, biwordIndexReader,
							biwordAnalyzer));
					sb.append(f.stream().map(ft -> ft + ",").collect(Collectors.joining()));
				}
			}
			data.add(queryText + "," + query.getFreq() + "," + sb.toString());
		}
		try (FileWriter fw = new FileWriter("features.csv")) {
			for (String line : data) {
				fw.write(line + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		for (int i = 0; i < cacheIndexReaderList.size(); i++) {
			cacheIndexReaderList.get(i).close();
			restIndexReaderList.get(i).close();
			biCacheIndexReaderList.get(i).close();
			biRestIndexReaderList.get(i).close();
		}
	}

}
