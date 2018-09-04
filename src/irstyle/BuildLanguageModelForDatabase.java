package irstyle;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;

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

public class BuildLanguageModelForDatabase {

	// Takes a set of tables and their cached tables, build 4 models: Tables-Word,
	// Tables-Biword, Cache-Word, Cache-Biword
	public static void main(String[] args) throws SQLException, IOException, ParseException {
		Options options = new Options();
		options.addOption(Option.builder("e").desc("The experiment inexp/inexr/msn").hasArg().build());
		CommandLineParser clp = new DefaultParser();
		CommandLine cl = clp.parse(options, args);
		String exp = cl.getOptionValue('e');
		String suffix;
		IRStyleExperiment experiment;
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
		try (Analyzer analyzer = new StandardAnalyzer(); Analyzer biwordAnalyzer = new BiwordAnalyzer()) {
			buildLmIndex(experiment, analyzer, suffix);
			suffix += "_bi";
			buildLmIndex(experiment, biwordAnalyzer, suffix);
		}
	}

	public static void buildLmIndex(IRStyleExperiment experiment, Analyzer analyzer, String suffix)
			throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(experiment.databaseType)) {
			// building index for LM
			Directory cacheDirectory = FSDirectory.open(Paths.get(experiment.dataDir + "lm_cache_" + suffix));
			IndexWriterConfig cacheConfig = Indexer.getIndexWriterConfig(analyzer).setOpenMode(OpenMode.CREATE);
			Directory restDirectory = FSDirectory.open(Paths.get(experiment.dataDir + "lm_rest_" + suffix));
			IndexWriterConfig restConfig = Indexer.getIndexWriterConfig(analyzer).setOpenMode(OpenMode.CREATE);
			try (IndexWriter cacheWriter = new IndexWriter(cacheDirectory, cacheConfig);
					IndexWriter restWriter = new IndexWriter(restDirectory, restConfig)) {
				for (int i = 0; i < experiment.tableNames.length; i++) {
					System.out.println("Indexing table " + experiment.tableNames[i]);
					Indexer.indexTable(dc, cacheWriter, experiment.tableNames[i], experiment.textAttribs[i],
							experiment.limits[i], experiment.popularity, false);
					Indexer.indexTable(dc, cacheWriter, experiment.tableNames[i], experiment.textAttribs[i],
							experiment.sizes[i] - experiment.limits[i], experiment.popularity, true);
				}
			}
		}
	}
}
