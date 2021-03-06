package irstyle;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.flexible.standard.parser.ParseException;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import database.DatabaseConnection;
import irstyle.api.IRStyleExperiment;
import irstyle.api.IRStyleExperimentHelper;
import irstyle.api.IRStyleKeywordSearch;
import irstyle.api.Indexer;
import irstyle.api.Params;
import irstyle.core.Relation;
import irstyle.core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import stackoverflow.QuestionDAO;
import stackoverflow.StackQueryingExperiment;

public class FindCache_NaiveTopk {

	private static final double popSum[] = { 15440314886.0, 52716653460.0, 1407925741807.0 };

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Option.builder("e").hasArg().desc("The experiment inexp/inexr/mrr").build());
		options.addOption(Option.builder("d").desc("Output debug info").build());
		CommandLineParser clp = new DefaultParser();
		CommandLine cl = clp.parse(options, args);
		List<ExperimentQuery> queries;
		int effectivenessMetric;
		IRStyleExperiment experiment;
		IRStyleExperimentHelper experimentHelper;
		if (cl.getOptionValue('e').equals("inexp")) {
			experiment = IRStyleExperiment.createWikiP20Experiment();
			experimentHelper = new Wiki_ExperimentHelper();
			queries = QueryServices.loadInexQueries();
			effectivenessMetric = 1;
		} else if (cl.getOptionValue('e').equals("inexr")) {
			experiment = IRStyleExperiment.createWikiRecExperiment();
			experimentHelper = new Wiki_ExperimentHelper();
			Params.N = 100;
			queries = QueryServices.loadInexQueries();
			effectivenessMetric = 2;
		} else if (cl.getOptionValue('e').equals("msn")) {
			experiment = IRStyleExperiment.createWikiMsnExperiment();
			experimentHelper = new Wiki_ExperimentHelper();
			queries = QueryServices.loadMsnQueriesAll();
			Params.N = 5;
			effectivenessMetric = 0;
		} else if (cl.getOptionValue('e').equals("stack")) {
			experiment = IRStyleExperiment.createStackExperiment();
			experimentHelper = new Stack_ExperimentHelper();
			StackQueryingExperiment sqe = new StackQueryingExperiment();
			List<QuestionDAO> questions = sqe.loadQuestionsFromTable("questions_s_test_train");
			queries = QuestionDAO.convertToExperimentQuery(questions);
			effectivenessMetric = 0;
		} else {
			throw new ParseException();
		}
		if (cl.hasOption('d')) {
			Params.DEBUG = true;
		}
		Collections.shuffle(queries, new Random(1));
		queries = queries.subList(0, 10);
		try (DatabaseConnection dc = new DatabaseConnection(experiment.databaseType)) {
			String[] tableNames = experiment.tableNames;
			Connection conn = dc.getConnection();
			String[] cacheTables = experiment.cacheNames;
			String[] selectTemplates = new String[tableNames.length];
			String[] insertTemplates = new String[tableNames.length];
			String[] indexPaths = new String[tableNames.length];
			// RAMDirectory[] fsDir = new RAMDirectory[tableNames.length];
			FSDirectory[] fsDir = new FSDirectory[tableNames.length];
			int[] pageSize = new int[tableNames.length];
			for (int i = 0; i < pageSize.length; i++) {
				pageSize[i] = (experiment.sizes[i] / 20);
			}
			IndexWriterConfig[] config = new IndexWriterConfig[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				try (Statement stmt = conn.createStatement()) {
					stmt.execute("drop table if exists " + cacheTables[i] + ";");
					stmt.execute(
							"create table " + cacheTables[i] + " as select id from " + tableNames[i] + " limit 0;");
					stmt.execute("create index id on " + cacheTables[i] + "(id);");
				}
				selectTemplates[i] = "select * from " + tableNames[i] + " order by " + experiment.popularity
						+ " desc limit ?, " + pageSize[i] + ";";
				insertTemplates[i] = "insert into " + cacheTables[i] + " (id) values (?);";
				indexPaths[i] = experiment.dataDir + cacheTables[i];
				config[i] = new IndexWriterConfig(new StandardAnalyzer());
				config[i].setSimilarity(new BM25Similarity());
				config[i].setRAMBufferSizeMB(1024);
				config[i].setOpenMode(OpenMode.CREATE);
			}
			IndexWriter indexWriters[] = new IndexWriter[tableNames.length];
			PreparedStatement selectSt[] = new PreparedStatement[tableNames.length];
			PreparedStatement insertSt[] = new PreparedStatement[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				fsDir[i] = FSDirectory.open(Paths.get(indexPaths[i]));
				indexWriters[i] = new IndexWriter(fsDir[i], config[i]);
				// ramDir[i] = new RAMDirectory();
				// indexWriters[i] = new IndexWriter(ramDir[i], config[i]);
				indexWriters[i].commit();
				selectSt[i] = conn.prepareStatement(selectTemplates[i]);
				insertSt[i] = conn.prepareStatement(insertTemplates[i]);
			}
			double acc = 0;
			double bestAcc = 0;
			int[] offset = { 0, 0, 0 };
			int[] bestOffset = { 0, 0, 0 };
			int loop = 1;
			String articleTable = cacheTables[0];
			String imageTable = cacheTables[1];
			String linkTable = cacheTables[2];
			String articleImageTable = experiment.relationTableNames[0];
			String articleLinkTable = experiment.relationTableNames[1];
			String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
					+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
					+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
					+ articleLinkTable + " " + linkTable;
			Vector<Relation> relations = experimentHelper.createRelations(articleTable, imageTable, linkTable,
					articleImageTable, articleLinkTable);
			IRStyleKeywordSearch.dropTupleSets(experimentHelper.getJdbcAccess(), relations);
			List<List<Document>> docsList = new ArrayList<List<Document>>();
			int[] lastPopularity = new int[tableNames.length];
			for (int i = 0; i < tableNames.length; i++) {
				selectSt[i].setInt(1, offset[i]);
				ResultSet rs = selectSt[i].executeQuery();
				List<Document> docs = new ArrayList<Document>();
				while (rs.next()) {
					int id = rs.getInt("id");
					String text = "";
					for (String attrib : experiment.textAttribs[i]) {
						text += rs.getString(attrib);
					}
					Document doc = new Document();
					doc.add(new StoredField("id", id));
					doc.add(new TextField("text", text, Store.NO));
					docs.add(doc);
					lastPopularity[i] = rs.getInt(experiment.popularity);
				}
				docsList.add(docs);
			}
			while (true) {
				System.out.println("Iteration " + loop++);
				System.out.println("  current offsets: " + Arrays.toString(offset));
				System.out.println("  current limits: " + (offset[0] + pageSize[0]) + " " + (offset[1] + pageSize[1])
						+ " " + (offset[2] + pageSize[2]));
				System.out.println("  current popularities: " + Arrays.toString(lastPopularity));
				System.out.println("  normalized popularities: " + lastPopularity[0] / popSum[0] + " "
						+ lastPopularity[1] / popSum[1] + " " + lastPopularity[2] / popSum[2]);
				double mPopularity = -1;
				int m = -1;
				for (int i = 0; i < lastPopularity.length; i++) {
					if (lastPopularity[i] / popSum[i] > mPopularity) {
						mPopularity = lastPopularity[i] / popSum[i];
						m = i;
					}
				}
				System.out.println("  Selected table = " + tableNames[m] + " with popularity = " + mPopularity);
				List<Document> docs = docsList.get(m);
				System.out.println("  reading new cache data..");
				for (Document doc : docs) {
					insertSt[m].setInt(1, Integer.parseInt(doc.get("id")));
					insertSt[m].addBatch();
				}
				System.out.println("  updating cache table..");
				insertSt[m].executeBatch();
				System.out.println("  updating cache index..");
				indexWriters[m].addDocuments(docs);
				indexWriters[m].flush();
				indexWriters[m].commit();
				// test partition!
				System.out.println("  testing new cache..");
				List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
				try (IndexReader articleReader = DirectoryReader.open(fsDir[0]);
						IndexReader imageReader = DirectoryReader.open(fsDir[1]);
						IndexReader linkReader = DirectoryReader.open(fsDir[2])) {
					System.out.println("  index sizes: " + articleReader.numDocs() + "," + imageReader.numDocs() + ","
							+ linkReader.numDocs());
					for (ExperimentQuery query : queries) {
						Schema sch = new Schema(schemaDescription);
						List<String> articleIds = IRStyleKeywordSearch.executeLuceneQuery(articleReader,
								query.getText(), Indexer.TEXT_FIELD, Indexer.ID_FIELD);
						List<String> imageIds = IRStyleKeywordSearch.executeLuceneQuery(imageReader, query.getText(),
								Indexer.TEXT_FIELD, Indexer.ID_FIELD);
						List<String> linkIds = IRStyleKeywordSearch.executeLuceneQuery(linkReader, query.getText(),
								Indexer.TEXT_FIELD, Indexer.ID_FIELD);
						Map<String, List<String>> relnamesValues = new HashMap<String, List<String>>();
						relnamesValues.put(articleTable, articleIds);
						relnamesValues.put(imageTable, imageIds);
						relnamesValues.put(linkTable, linkIds);
						IRStyleQueryResult result = IRStyleKeywordSearch.executeIRStyleQuery(
								experimentHelper.getJdbcAccess(), sch, relations, query, relnamesValues);
						queryResults.add(result);
					}
					acc = effectiveness(queryResults, effectivenessMetric, null);
					System.out.println("  new accuracy = " + acc);

				}
				offset[m] += pageSize[m];
				System.out.println("  current offsets: " + Arrays.toString(offset));
				if (acc > bestAcc) {
					bestAcc = acc;
					bestOffset = offset.clone();
				}
				if ((bestAcc - acc) > 0.005 && (m == 0)) {
					System.out.println("  time to break");
					System.out.println("======================================");
					break;
				}
				if (lastPopularity[0] == -1 && lastPopularity[1] == -1 && lastPopularity[2] == -1) {
					System.out.println("  breaking due negative popularity");
					break;
				}
				// update buffer
				selectSt[m].setInt(1, offset[m]);
				ResultSet rs = selectSt[m].executeQuery();
				lastPopularity[m] = -1;
				docs = new ArrayList<Document>();
				while (rs.next()) {
					int id = rs.getInt("id");
					String text = "";
					for (String attrib : experiment.textAttribs[m]) {
						text += rs.getString(attrib);
					}
					Document doc = new Document();
					doc.add(new StoredField("id", id));
					doc.add(new TextField("text", text, Store.NO));
					docs.add(doc);
					lastPopularity[m] = rs.getInt(experiment.popularity);
				}
				docsList.remove(m);
				docsList.add(m, docs);
			}
			System.out.println("Offsets for articles, images, links = " + Arrays.toString(bestOffset));
			double[] percent = new double[bestOffset.length];
			for (int i = 0; i < bestOffset.length; i++) {
				percent[i] = bestOffset[i] / (double) experiment.sizes[i];
			}
			System.out.println("Best found sizes = " + Arrays.toString(percent));
			for (int i = 0; i < tableNames.length; i++) {
				indexWriters[i].close();
				selectSt[i].close();
				insertSt[i].close();
			}
		}
	}

	public static double effectiveness(List<IRStyleQueryResult> queryResults, int mode,
			Map<ExperimentQuery, Integer> queryRelCountMap) {
		double acc = 0;
		for (IRStyleQueryResult qr : queryResults) {
			qr.dedup();
			if (mode == 0) {
				acc += qr.rrank();
			} else if (mode == 1) {
				acc += qr.p20();
			} else {
				acc += qr.recall();
			}
		}
		acc /= queryResults.size();
		return acc;
	}

}
