package stackoverflow.irstyle;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
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
import irstyle.api.Indexer;
import query.ExperimentQuery;
import stackoverflow.QuestionDAO;
import stackoverflow.StackQueryingExperiment;

public class RunFeatureExtractionForMultiTables {

	public static final Logger LOGGER = Logger.getLogger(RunFeatureExtractionForMultiTables.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		List<String> argList = Arrays.asList(args);
		StackQueryingExperiment sqe = new StackQueryingExperiment();
		List<QuestionDAO> questions = sqe.loadQuestionsFromTable("questions_s_test_train");
		List<ExperimentQuery> queries = QuestionDAO.convertToExperimentQuery(questions);
		List<IndexReader> cacheIndexReaderList = new ArrayList<IndexReader>();
		List<IndexReader> restIndexReaderList = new ArrayList<IndexReader>();
		List<IndexReader> biCacheIndexReaderList = new ArrayList<IndexReader>();
		List<IndexReader> biRestIndexReaderList = new ArrayList<IndexReader>();
		LOGGER.log(Level.INFO, "loading popularity indices..");
		Map<String, TokenPopularity> cacheTermPopularity;
		Map<String, TokenPopularity> restTermPopularity;
		Map<String, TokenPopularity> biwordCachePopularity;
		Map<String, TokenPopularity> biwordRestPopularity;
		String suffix = "mrr";
		if (argList.contains("-eff")) {
			Collections.shuffle(queries, new Random(1));
			queries = queries.subList(0, 20);
		}
		for (String table : StackConstants.tableName) {
			String indexPath = StackConstants.DATA_STACK + "ml_" + table + "_cache_" + suffix;
			cacheIndexReaderList.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
			indexPath = StackConstants.DATA_STACK + "ml_" + table + "_rest_" + suffix;
			restIndexReaderList.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
			indexPath = StackConstants.DATA_STACK + "ml_" + table + "_cache_" + suffix + "_bi";
			biCacheIndexReaderList.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
			indexPath = StackConstants.DATA_STACK + "ml_" + table + "_rest_" + suffix + "_bi";
			biRestIndexReaderList.add(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
		}
		LOGGER.log(Level.INFO, "loading popularity indices..");
		cacheTermPopularity = TokenPopularity
				.loadTokenPopularities(StackConstants.DATA_STACK + "lm_cache_" + suffix + "_text_pop_index" + ".csv");
		restTermPopularity = TokenPopularity
				.loadTokenPopularities(StackConstants.DATA_STACK + "lm_rest_" + suffix + "_text_pop_index" + ".csv");
		biwordCachePopularity = TokenPopularity.loadTokenPopularities(
				StackConstants.DATA_STACK + "lm_cache_" + suffix + "_bi" + "_text_pop_index" + ".csv");
		biwordRestPopularity = TokenPopularity
				.loadTokenPopularities(StackConstants.DATA_STACK + "lm_rest_" + suffix + "_bi" + "_text_pop_index" + ".csv");
		FeatureExtraction wqde = new FeatureExtraction(Indexer.WEIGHT_FIELD);
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
			for (int j = 0; j < StackConstants.textAttribs[i].length; j++) {
				final String featSuffix = i + "_" + j;
				fileHeader.append(Arrays.asList(featureNames).stream().map(ft -> ft + "_" + featSuffix + ",")
						.collect(Collectors.joining()));
			}
		}
		data.add("query,freq," + fileHeader);
		Analyzer biwordAnalyzer = new BiwordAnalyzer();
		Analyzer analyzer = new StandardAnalyzer();
		long time = 0;
		for (ExperimentQuery query : queries) {
			String queryText = query.getText();
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < cacheIndexReaderList.size(); i++) {
				IndexReader indexReader = cacheIndexReaderList.get(i);
				IndexReader restIndexReader = restIndexReaderList.get(i);
				IndexReader biwordIndexReader = biCacheIndexReaderList.get(i);
				IndexReader biwordRestIndexReader = biRestIndexReaderList.get(i);
				for (String attrib : StackConstants.textAttribs[i]) {
					long start = System.currentTimeMillis();
					List<Double> f = new ArrayList<Double>();
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
					time += System.currentTimeMillis() - start;
				}
			}
			data.add(queryText + "," + query.getFreq() + "," + sb.toString());
		}
		System.out.println("time per query = " + time / queries.size());
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
