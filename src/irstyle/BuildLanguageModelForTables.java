package irstyle;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import indexing.BiwordAnalyzer;
import irstyle.api.IRStyleExperiment;
import irstyle.api.Indexer;

public class BuildLanguageModelForTables {
	
	// Takes a set of tables and their cached tables, build a language model for each cache and rest
	public static void main(String[] args) throws SQLException, IOException, ParseException {
		Options options = new Options();
		options.addOption(Option.builder("e").desc("The experiment inexp/inexr/msn").hasArg().build());
		options.addOption(Option.builder("b").desc("Enables biword indexing").hasArg(false).build());
		CommandLineParser clp = new DefaultParser();
		CommandLine cl = clp.parse(options, args);
		String exp = cl.getOptionValue('e');
		String suffix;
		final IRStyleExperiment experiment;
		if (exp.equals("-inexp")) {
			experiment = IRStyleExperiment.createWikiP20Experiment();
			suffix = "p20";
		} else if (exp.equals("-inexr")) {
			experiment = IRStyleExperiment.createWikiRecExperiment();
			suffix = "rec";
		} else if (exp.equals("msn")) {
			experiment = IRStyleExperiment.createWikiMsnExperiment();
			suffix = "mrr";
		} else if (exp.equals("stack")) {
			experiment = IRStyleExperiment.createStackExperiment();
			suffix = "mrr";
		} else {
			throw new ParseException("Experiment is not recognized!");
		}
		Analyzer analyzer;
		if (cl.hasOption('b')) {
			analyzer = new BiwordAnalyzer();
			suffix += "_bi";
		} else {
			analyzer = new StandardAnalyzer();
		}
		String finalSuffix = suffix;
		int[] tableIndex = { 0, 1, 2 };
		Arrays.stream(tableIndex).parallel().forEach(tableNo -> {
			Path cacheIndexPath = Paths
					.get(experiment.dataDir + "ml_" + experiment.tableNames[tableNo] + "_cache_" + finalSuffix);
			Path restIndexPath = Paths
					.get(experiment.dataDir + "ml_" + experiment.tableNames[tableNo] + "_rest_" + finalSuffix);
			try (DatabaseConnection dc = new DatabaseConnection(experiment.databaseType);
					Directory cacheIndexDir = FSDirectory.open(cacheIndexPath);
					Directory restIndexDir = FSDirectory.open(restIndexPath);) {
				IndexWriterConfig cacheIndexWriterConfig = Indexer.getIndexWriterConfig(analyzer)
						.setOpenMode(OpenMode.CREATE);
				IndexWriterConfig restIndexWriterConfig = Indexer.getIndexWriterConfig(analyzer)
						.setOpenMode(OpenMode.CREATE);
				try (IndexWriter cacheIndexWriter = new IndexWriter(cacheIndexDir, cacheIndexWriterConfig);
						IndexWriter restIndexWriter = new IndexWriter(restIndexDir, restIndexWriterConfig)) {
					Indexer.indexTableAttribs(dc, cacheIndexWriter, experiment.tableNames[tableNo],
							experiment.textAttribs[tableNo], experiment.limits[tableNo], experiment.popularity, false);
					Indexer.indexTableAttribs(dc, cacheIndexWriter, experiment.tableNames[tableNo],
							experiment.textAttribs[tableNo], experiment.sizes[tableNo] - experiment.limits[tableNo],
							experiment.popularity, true);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		analyzer.close();
	}

}
