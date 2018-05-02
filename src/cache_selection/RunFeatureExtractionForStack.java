package cache_selection;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
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
import stackoverflow.QuestionDAO;
import stackoverflow.StackIndexer;
import stackoverflow.StackQuery;

public class RunFeatureExtractionForStack {

	public static final Logger LOGGER = Logger.getLogger(RunFeatureExtractionForStack.class.getName());

	public static void main(String[] args) throws IOException, SQLException {

		Options options = new Options();
		Option expNumberOption = new Option("exp", true, "Number of experiment");
		expNumberOption.setRequired(true);
		options.addOption(expNumberOption);
		Option totalExpNumberOption = new Option("total", true, "Total number of experiments");
		totalExpNumberOption.setRequired(true);
		options.addOption(totalExpNumberOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;

		try {
			cl = clp.parse(options, args);
			String exp = cl.getOptionValue("exp");
			String totalExp = cl.getOptionValue("total");
			String indexBase = "/data/ghadakcv/stack_index_s/";
			String biwordIndexBase = "/data/ghadakcv/stack_index_s_bi/";
			Path indexPath = Paths.get(indexBase + exp);
			Path globalIndexPath = Paths.get(indexBase + totalExp);
			Path biwordIndexPath = Paths.get(biwordIndexBase + exp);
			Path globalBiwordIndexPath = Paths.get(biwordIndexBase + totalExp);
			String weightField = StackIndexer.VIEW_COUNT_FIELD;
			String bodyField = StackIndexer.BODY_FIELD;
			StackQuery sqsr = new StackQuery();
			List<QuestionDAO> queries = sqsr.loadQueries("questions_s_test_train");
			FeatureExtraction wqde = new FeatureExtraction(weightField);
			LOGGER.log(Level.INFO, "loading popularity indices..");
			Map<String, TokenPopularity> termTitlePopularity = TokenPopularity
					.loadTokenPopularities(indexPath + "_Body_pop_fast" + ".csv");
			Map<String, TokenPopularity> biwordTitlePopularity = TokenPopularity
					.loadTokenPopularities(biwordIndexPath + "_Body_pop_fast" + ".csv");
			LOGGER.log(Level.INFO, "loading done!");
			LOGGER.log(Level.INFO, "extracting features..");
			List<String> data = new ArrayList<String>();
			String[] featureNames = { "query", "covered_t", "mean_df_t", "min_df_t", "mean_mean_pop_t",
					"mean_min_pop_t", "min_mean_pop_t", "min_min_pop_t", "qll_t", "covered_t_bi", "mean_df_t_bi",
					"min_df_t_bi", "mean_mean_pop_t_bi", "mean_min_pop_t_bi", "min_mean_pop_t_bi", "min_min_pop_t_bi",
					"ql_t_bi", "qll_t_bi" };
			data.add(Arrays.asList(featureNames).stream().map(ft -> ft + ",").collect(Collectors.joining()));
			try (IndexReader indexReader = DirectoryReader.open(FSDirectory.open(indexPath));
					IndexReader globalIndexReader = DirectoryReader.open(FSDirectory.open(globalIndexPath));
					IndexReader biwordIndexReader = DirectoryReader.open(FSDirectory.open(biwordIndexPath));
					IndexReader globalBiwordIndexReader = DirectoryReader.open(FSDirectory.open(globalBiwordIndexPath));
					Analyzer biwordAnalyzer = new BiwordAnalyzer();
					Analyzer analyzer = new StandardAnalyzer()) {
				for (QuestionDAO query : queries) {
					String queryText = query.text;
					List<Double> f = new ArrayList<Double>();
					f.add(wqde.coveredTokenRatio(indexReader, queryText, bodyField, analyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(indexReader, queryText, bodyField, analyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(indexReader, queryText, bodyField, analyzer));
					List<Double> averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(termTitlePopularity,
							queryText, bodyField, analyzer);
					f.addAll(averageTokenDocPopularity);
					f.add(wqde.queryLikelihood(indexReader, queryText, bodyField, globalIndexReader, analyzer));
					f.add(wqde.queryLogLikelihood(indexReader, queryText, bodyField, globalIndexReader, analyzer));
					f.add(wqde.coveredTokenRatio(biwordIndexReader, queryText, bodyField, biwordAnalyzer));
					f.add(wqde.meanNormalizedTokenDocumentFrequency(biwordIndexReader, queryText, bodyField,
							biwordAnalyzer));
					f.add(wqde.minNormalizedTokenDocumentFrequency(biwordIndexReader, queryText, bodyField,
							biwordAnalyzer));
					averageTokenDocPopularity = wqde.fastTokenPopularityFeatures(biwordTitlePopularity, queryText,
							bodyField, biwordAnalyzer);
					f.addAll(averageTokenDocPopularity);
					f.add(wqde.queryLikelihood(biwordIndexReader, queryText, bodyField, globalBiwordIndexReader,
							biwordAnalyzer));
					f.add(wqde.queryLogLikelihood(biwordIndexReader, queryText, bodyField, globalBiwordIndexReader,
							biwordAnalyzer));
					data.add(queryText + "," + f.stream().map(ft -> ft + ",").collect(Collectors.joining()));
				}
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}

			try (FileWriter fw = new FileWriter("/data/ghadakcv/stack_results/stack_feat/" + exp + ".csv")) {
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
