package irstyle;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import irstyle.api.IRStyleKeywordSearch;
import irstyle.api.Indexer;
import irstyle.api.Params;
import irstyle.core.ExecPrepared;
import irstyle.core.JDBCaccess;
import irstyle.core.Relation;
import irstyle.core.Schema;
import query.ExperimentQuery;
import query.QueryServices;

public class RunWikiCacheSearch {

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Option.builder("e").hasArg().desc("The experiment inexp/inexr/mrr").build());
		options.addOption(Option.builder("c").desc("Use cache").build());
		options.addOption(Option.builder("f").desc("Efficiency experiment").build());
		options.addOption(Option.builder("k").desc("The k in tok-k").hasArg().build());
		options.addOption(Option.builder("t").desc("TS size threshold").hasArg().build());
		options.addOption(Option.builder("s").desc("Score thresholding").build());
		options.addOption(Option.builder("d").desc("Output debug info").build());
		CommandLineParser clp = new DefaultParser();
		CommandLine cl;
		String cacheNameSuffix;
		List<ExperimentQuery> queries;
		String outputFileName = "result";
		cl = clp.parse(options, args);
		if (cl.getOptionValue('e').equals("inexp")) {
			cacheNameSuffix = "p20";
			queries = QueryServices.loadInexQueries();
		} else if (cl.getOptionValue('e').equals("inexr")) {
			if (!cl.hasOption('f')) {
				Params.N = 100;
			}
			cacheNameSuffix = "rec";
			queries = QueryServices.loadInexQueries();
		} else {
			cacheNameSuffix = "mrr";
			queries = QueryServices.loadMsnQueriesAll();
		}
		outputFileName += "_" + cacheNameSuffix;
		boolean justUseCache = false;
		String articleIndexPath;
		String imageIndexPath;
		String linkIndexPath;
		if (cl.hasOption('c')) {
			justUseCache = true;
			outputFileName += "_cache";
			articleIndexPath = WikiConstants.WIKI_DATA_DIR + "sub_article_wiki13_" + cacheNameSuffix;
			imageIndexPath = WikiConstants.WIKI_DATA_DIR + "sub_image_pop_" + cacheNameSuffix;
			linkIndexPath = WikiConstants.WIKI_DATA_DIR + "sub_link_pop_" + cacheNameSuffix;
		} else {
			articleIndexPath = WikiConstants.WIKI_DATA_DIR + "tbl_article_wiki13/100";
			imageIndexPath = WikiConstants.WIKI_DATA_DIR + "tbl_image_pop/100";
			linkIndexPath = WikiConstants.WIKI_DATA_DIR + "tbl_link_pop/100";
			outputFileName += "_full";
		}
		if (cl.hasOption('f')) {
			Collections.shuffle(queries, new Random(1));
			queries = queries.subList(0, 20);
			outputFileName += "_eff";
		}
		if (cl.hasOption('d')) {
			Params.DEBUG = true;
		}
		outputFileName += ".csv";
		Params.MAX_TS_SIZE = Integer.parseInt(cl.getOptionValue("t", "10000"));
		Params.N = Integer.parseInt(cl.getOptionValue("k", "20"));
		Params.useScoreThresholding = cl.hasOption("s");
		System.out.println("setting: \n" + Params.getDescriptor());
		JDBCaccess jdbcacc = IRStyleWikiHelper.jdbcAccess();
		IRStyleKeywordSearch.dropAllTuplesets(jdbcacc);
		List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
		try (IndexReader articleReader = DirectoryReader.open(FSDirectory.open(Paths.get(articleIndexPath)));
				IndexReader imageReader = DirectoryReader.open(FSDirectory.open(Paths.get(imageIndexPath)));
				IndexReader linkReader = DirectoryReader.open(FSDirectory.open(Paths.get(linkIndexPath)))) {
			long time = 0;
			int cacheUseCount = 0;
			long selectionTime = 0;
			long luceneTime = 0;
			long tuplesetTime = 0;
			double recall = 0;
			double p20 = 0;
			double mrr = 0;
			for (int exec = 0; exec < Params.numExecutions; exec++) {
				int loop = 1;
				for (ExperimentQuery query : queries) {
					System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
					Vector<String> allkeyw = new Vector<String>();
					// escaping single quotes
					allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
					String articleTable = "tbl_article_wiki13";
					String imageTable = "tbl_image_pop";
					String linkTable = "tbl_link_pop";
					String articleImageTable = "tbl_article_image_09";
					String articleLinkTable = "tbl_article_link_09";
					long start = System.currentTimeMillis();
					if (justUseCache) {
						cacheUseCount++;
						articleTable = "sub_article_wiki13";
						articleImageTable = "sub_article_image_09";
						imageTable = "sub_image_pop";
						articleLinkTable = "sub_article_link_09";
						linkTable = "sub_link_pop";
					}
					selectionTime += System.currentTimeMillis() - start;
					String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
							+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
							+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
							+ articleLinkTable + " " + linkTable;
					Schema sch = new Schema(schemaDescription);
					if (Params.DEBUG) {
						System.out.println(" Using tables: " + articleTable + " " + articleImageTable + " " + imageTable
								+ " " + articleLinkTable + " " + linkTable);
						System.out.println(
								" Using indices: " + articleIndexPath + " " + imageIndexPath + " " + linkIndexPath);
					}
					Vector<Relation> relations = IRStyleWikiHelper.createRelations(articleTable, imageTable, linkTable,
							articleImageTable, articleLinkTable, jdbcacc.conn);
					start = System.currentTimeMillis();
					List<String> articleIds = IRStyleKeywordSearch.executeLuceneQuery(articleReader, query.getText(),
							Indexer.TEXT_FIELD, Indexer.ID_FIELD);
					List<String> imageIds = IRStyleKeywordSearch.executeLuceneQuery(imageReader, query.getText(),
							Indexer.TEXT_FIELD, Indexer.ID_FIELD);
					List<String> linkIds = IRStyleKeywordSearch.executeLuceneQuery(linkReader, query.getText(),
							Indexer.TEXT_FIELD, Indexer.ID_FIELD);
					luceneTime += (System.currentTimeMillis() - start);
					if (Params.DEBUG) {
						System.out.printf(" |TS_article| = %d |TS_images| = %d |TS_links| = %d", articleIds.size(),
								imageIds.size(), linkIds.size());
					}
					Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
					relnamesValues.put(articleTable, articleIds);
					relnamesValues.put(imageTable, imageIds);
					relnamesValues.put(linkTable, linkIds);
					IRStyleQueryResult result = IRStyleKeywordSearch.executeIRStyleQuery(jdbcacc, sch, relations, query,
							relnamesValues);
					if (Params.DEBUG) {
						System.out.println(" table scan percentage = " + (double) ExecPrepared.lastGenQueries
								/ (articleIds.size() * imageIds.size() * linkIds.size()) + "%");
					}
					result.dedup();
					tuplesetTime += result.tuplesetTime;
					time += luceneTime + result.execTime;
					recall += result.recall();
					p20 += result.p20();
					mrr += result.rrank();
					queryResults.add(result);
				}
			}
			selectionTime /= (queries.size() * Params.numExecutions);
			luceneTime /= (queries.size() * Params.numExecutions);
			tuplesetTime /= (queries.size() * Params.numExecutions);
			time /= queries.size() * Params.numExecutions;
			System.out.println("average cache selection time = " + selectionTime + " (ms)");
			System.out.println("average lucene time = " + luceneTime + " (ms)");
			System.out.println("average tupleset time = " + tuplesetTime + " (ms)");
			System.out.println("average just search time = " + (time - tuplesetTime) + " (ms)");
			System.out.println("average total time  = " + time + " (ms)");
			System.out.println("number of cache hits: " + cacheUseCount + "/" + queries.size());
			System.out.println("recall = " + recall / queries.size());
			System.out.println("p20 = " + p20 / queries.size());
			System.out.println("mrr = " + mrr / queries.size());
			System.out.println("avergae article TS size: "
					+ IRStyleKeywordSearch.aggregateArticleTuplesetSize / IRStyleKeywordSearch.counter);
			System.out.println("average gen queries: " + ExecPrepared.totalGenQueries / ExecPrepared.execCount);
			IRStyleKeywordSearch.printResults(queryResults, outputFileName);
		}
	}

}
