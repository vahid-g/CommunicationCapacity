package cache_selection;

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import indexing.BiwordAnalyzer;
import indexing.popularity.TokenPopularity;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiFileIndexer;
import wiki13.WikiFilesPaths;

public class RunFeatureExtractionForWiki {

	public static final Logger LOGGER = Logger.getLogger(RunFeatureExtractionForWiki.class.getName());

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
			FeatureExtraction wqde = new FeatureExtraction(WikiFileIndexer.WEIGHT_ATTRIB);
			LOGGER.log(Level.INFO, "loading popularity indices..");
			Map<String, TokenPopularity> termTitlePopularity = TokenPopularity
					.loadTokenPopularities(indexPath + "_title_pop_fast_tokens" + ".csv");
			Map<String, TokenPopularity> termContentPopularity = TokenPopularity
					.loadTokenPopularities(indexPath + "_content_pop_fast_tokens" + ".csv");
			Map<String, TokenPopularity> biwordTitlePopularity = TokenPopularity
					.loadTokenPopularities(biwordIndexPath + "_title_pop_fast_tokens" + ".csv");
			Map<String, TokenPopularity> biwordContentPopularity = TokenPopularity
					.loadTokenPopularities(biwordIndexPath + "_content_pop_fast_tokens" + ".csv");
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
}
