package wiki13;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import database.DatabaseConnection;
import database.DatabaseType;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;

public class WikiRelationalEfficiencyExperiment {

	private static Logger LOGGER = Logger.getLogger(WikiRelationalEfficiencyExperiment.class.getName());
	private static WikiFilesPaths PATHS = WikiFilesPaths.getMaplePaths();

	public static void main(String[] args) throws SQLException {
		Options options = new Options();
		Option expOption = new Option("exp", true, "experiment number");
		expOption.setRequired(true);
		options.addOption(expOption);
		Option querysetOption = new Option("queryset", true, "specifies the query log (msn/inex)");
		querysetOption.setRequired(true);
		options.addOption(querysetOption);
		Option gammaOption = new Option("gamma", true, "the weight of title field");
		options.addOption(gammaOption);
		CommandLineParser clp = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cl;
		try {
			cl = clp.parse(options, args);
			List<ExperimentQuery> queries;
			if (cl.getOptionValue("queryset").equals("msn")) {
				queries = QueryServices.loadMsnQueries(PATHS.getMsnQueryFilePath(), PATHS.getMsnQrelFilePath());
			} else {
				queries = QueryServices.loadInexQueries(PATHS.getInexQueryFilePath(), PATHS.getInexQrelFilePath());
			}
			Collections.shuffle(queries, new Random(1l));
			queries = queries.subList(0, 20);
			WikiRelationalEfficiencyExperiment wmree = new WikiRelationalEfficiencyExperiment();
			long gamma = Long.parseLong(cl.getOptionValue("gamma"));
			wmree.queryEfficiencyExperiment("normal", cl.getOptionValue("exp"), queries, gamma);
		} catch (org.apache.commons.cli.ParseException e) {
			LOGGER.log(Level.SEVERE, e.getMessage());
			formatter.printHelp("", options);
		}
	}

	void debug() {
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(PATHS.getMsnQueryFilePath(),
				PATHS.getMsnQrelFilePath());
		queries = queries.subList(0, 10);
		double[] tmp = new double[10];
		for (int j = 0; j < 2; j++) {
			for (int i = 91; i > 0; i -= 10) {
				String indexPath = PATHS.getIndexBase() + i;
				long startTime = System.currentTimeMillis();
				WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.1f);
				long spentTime = System.currentTimeMillis() - startTime;
				tmp[i / 10] = spentTime / queries.size();
			}
			LOGGER.log(Level.INFO, Arrays.toString(tmp));
		}
	}

	void queryEfficiencyExperiment(String mode, String subset, List<ExperimentQuery> queries, long gamma) {
		String subsetIndexPath = PATHS.getIndexBase() + subset;
		String indexPath = PATHS.getIndexBase() + "100";
		try (DatabaseConnection dm = new DatabaseConnection(DatabaseType.WIKIPEDIA)) {
			String subsetPrefix = "";
			String articleTable = "sub_article_" + subset;
			String imageRelTable = "sub_article_image_" + subset;
			String linkRelTable = "sub_article_link_" + subset;
			String imageTable = "sub_image_" + subset;
			String linkTable = "sub_link_" + subset;
			switch (mode) {
			case "normal":
				subsetPrefix = "SELECT a.id FROM " + articleTable + " a left join " + imageRelTable
						+ " i on a.id = i.article_id left join " + imageTable + " ii on i.image_id = ii.id left join "
						+ linkRelTable + " l on a.id = l.article_id left join " + linkTable
						+ " ll on l.article_id = ll.id WHERE a.id in %s;";
				break;
			case "denorm":
				subsetPrefix = "SELECT a.id FROM den_article_link_1 WHERE a.id in %s;";
				break;
			case "memory":
				articleTable = "mem_article_" + subset;
				imageRelTable = "mem_article_image_" + subset;
				linkRelTable = "mem_article_link_" + subset;
				imageTable = "mem_image_" + subset;
				linkTable = "mem_link_" + subset;
				subsetPrefix = "SELECT a.id FROM " + articleTable + " a left join " + imageRelTable
						+ " i on a.id = i.article_id left join " + imageTable + " ii on i.image_id = ii.id left join "
						+ linkRelTable + " l on a.id = l.article_id left join " + linkTable
						+ " ll on l.article_id = ll.id WHERE a.id in %s;";
				break;
			default:
				LOGGER.log(Level.SEVERE, "Mode is not correct");
				return;
			}
			articleTable = "tbl_article_wiki13";
			imageRelTable = "tbl_article_image_09";
			linkRelTable = "tbl_article_link_09";
			imageTable = "tbl_image_09";
			linkTable = "tbl_link_09";
			String dbPrefix = "SELECT a.id FROM " + articleTable + " a left join " + imageRelTable
					+ " i on a.id = i.article_id left join " + imageTable + " ii on i.image_id = ii.id left join "
					+ linkRelTable + " l on a.id = l.article_id left join " + linkTable
					+ " ll on l.article_id = ll.id WHERE a.id in %s;";
			double[] time = new double[4];
			int iterCount = 3;
			for (int i = 0; i < iterCount; i++) {
				double dbTimes[] = measureQueryEfficiency(indexPath, dm, queries, dbPrefix, gamma);
				double subsetTimes[] = measureQueryEfficiency(subsetIndexPath, dm, queries, subsetPrefix, gamma);
				time[0] += subsetTimes[0];
				time[1] += subsetTimes[1];
				time[2] += dbTimes[0];
				time[3] += dbTimes[1];
			}
			LOGGER.log(Level.INFO, "Average per query time (ms) after " + iterCount + " interations:");
			LOGGER.log(Level.INFO, "subset: " + time[0] / iterCount + "," + time[1] / iterCount + ","
					+ (time[0] + time[1]) / iterCount);
			LOGGER.log(Level.INFO,
					"db: " + time[2] / iterCount + "," + time[3] / iterCount + "," + (time[2] + time[3]) / iterCount);
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	double[] measureQueryEfficiency(String indexPath, DatabaseConnection dm, List<ExperimentQuery> queries,
			String queryPrefix, float gamma) throws SQLException {
		long startTime = System.currentTimeMillis();
		List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, gamma);
		long middleTime = System.currentTimeMillis();
		int counter = 0;
		for (QueryResult result : results) {
			List<String> ids = result.getTopDocuments().subList(0, Math.min(result.getTopDocuments().size(), 20))
					.stream().map(t -> t.id).collect(Collectors.toList());
			String query = String.format(queryPrefix, ids.toString().replace('[', '(').replace(']', ')'));
			LOGGER.log(Level.FINE, query);
			long tmp = System.currentTimeMillis();
			long queryTime = measureQueryTime(dm.getConnection(), query);
			LOGGER.log(Level.FINE, counter++ + ": " + ids.size() + ": " + (System.currentTimeMillis() - tmp) + ": "
					+ ": " + queryTime + ": " + query);
		}
		long endTime = System.currentTimeMillis();
		return new double[] { (middleTime - startTime) / queries.size(), (endTime - middleTime) / queries.size() };
	}

	public long measureQueryTime(java.sql.Connection connection, String query) throws SQLException {
		try (Statement stmt = connection.createStatement()) {
			long begin = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(query);
			long end = System.currentTimeMillis();
			int counter = 0;
			while (rs.next()) {
				counter++;
				rs.getString("id");
			}
			LOGGER.log(Level.FINE, "fetch size: " + counter);
			return end - begin;
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return -1;
	}

}
