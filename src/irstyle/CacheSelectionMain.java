package irstyle;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import cache_selection.FeatureExtraction;
import irstyle_core.JDBCaccess;
import irstyle_core.MIndexAccess;
import irstyle_core.Relation;
import irstyle_core.Result;
import irstyle_core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiFileIndexer;
import wiki13.WikiFilesPaths;

public class CacheSelectionMain {

	static FileOutputStream output;
	Random ran = new Random();

	public static String TUPLESET_PREFIX = "TS2";
	public static int MAX_GENERATED_CNS = 50;

	public static void main(String[] args) throws IOException {

		// start input
		int maxCNsize = 5;
		int N = 100;
		boolean allKeywInResults = false;
		boolean parallel = true;
		JDBCaccess jdbcacc = IRStyleMain.jdbcAccess();
		WikiFilesPaths paths = null;
		paths = WikiFilesPaths.getMaplePaths();
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
				paths.getMsnQrelFilePath());
		IRStyleMain.dropTupleSets(jdbcacc);
		try (FileWriter fw = new FileWriter("result.csv")) {
			try (IndexReader articleReader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
					IndexReader articleCacheReader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
					IndexReader articleRestReader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
					IndexReader imageReader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
					IndexReader imageCacheReader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
					IndexReader imageRestReader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
					IndexReader linkReader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
					IndexReader linkCacheReader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
					IndexReader linkRestReader = DirectoryReader.open(FSDirectory.open(Paths.get("")))) {
				int loop = 1;
				for (ExperimentQuery query : queries) {
					Vector<String> allkeyw = new Vector<String>();
					allkeyw.addAll(Arrays.asList(query.getText().split(" ")));
					// allkeyw.add("jimmy");
					// allkeyw.add("hoffa");
					System.out
							.println("processing " + allkeyw + " " + ((100 * loop++) / queries.size()) + "% completed");

					String articleTable = "tbl_article_09";
					String imageTable = "tbl_image_09_tk";
					String linkTable = "tbl_link_09";
					String articleImageTable = "tbl_article_image_09";
					String articleLinkTable = "tbl_article_link_09";
					if (useCache(query.getText(), articleCacheReader, articleReader, articleRestReader)) {
						articleTable = "mem_article_09";
					}
					if (useCache(query.getText(), imageCacheReader, imageReader, imageRestReader)) {
						imageTable = "mem_image_09";
					}
					if (useCache(query.getText(), linkCacheReader, linkReader, linkRestReader)) {
						linkTable = "mem_link_09";
					}

					String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
							+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
							+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
							+ articleLinkTable + " " + linkTable;
					Schema sch = new Schema(schemaDescription);
					Vector<Relation> relations = createRelations(articleTable, imageTable, linkTable);

					// access master index and create tuple sets
					MIndexAccess MIndx = new MIndexAccess(relations);
					long time3 = System.currentTimeMillis();
					MIndx.createTupleSets2(sch, allkeyw, jdbcacc.conn);
					long time4 = System.currentTimeMillis();

					System.out.println("time to create tuple sets=" + (time4 - time3) + " (ms)");
					time3 = System.currentTimeMillis();
					/** returns a vector of instances (tuple sets) */ // P1
					Vector<?> CNs = sch.getCNs(maxCNsize, allkeyw, sch, MIndx);
					// also prune identical CNs with P2 in place of
					time4 = System.currentTimeMillis();
					// IRStyleMain.writetofile("#CNs=" + CNs.size() + " Time to get CNs=" + (time4 -
					// time3) + "\r\n");
					System.out.println("#CNs=" + CNs.size() + " Time to get CNs=" + (time4 - time3) + " (ms)");
					ArrayList<Result> results = new ArrayList<Result>(1);
					int exectime = 0;
					if (!parallel) {
						exectime = IRStyleMain.methodB(N, allKeywInResults, relations, allkeyw, CNs, results,
								jdbcacc);
					} else {
						exectime = IRStyleMain.methodC(N, allKeywInResults, relations, allkeyw, CNs, results,
								jdbcacc);
					}
					IRStyleMain.dropTupleSets(jdbcacc);
					double mrr = IRStyleMain.mrr(results, query);
					System.out.println(" R-rank = " + mrr);
					fw.write(query.getId() + "," + query.getText().replaceAll(",", " ") + "," + mrr + "," + exectime
							+ "\n");
				}
			}
		}

	}

	static boolean useCache(String query, IndexReader cacheIndexReader, IndexReader globalIndexReader,
			IndexReader restIndexReader) throws IOException {
		FeatureExtraction fe = new FeatureExtraction(WikiFileIndexer.WEIGHT_ATTRIB);
		double ql_cache = 0;
		double ql_rest = 0;
		ql_cache = fe.queryLikelihood(cacheIndexReader, query, WikiTableIndexer.TEXT_FIELD, globalIndexReader,
				new StandardAnalyzer());
		ql_rest = fe.queryLikelihood(restIndexReader, query, WikiTableIndexer.TEXT_FIELD, globalIndexReader,
				new StandardAnalyzer());
		return (ql_cache >= ql_rest);
	}

	static Vector<Relation> createRelations(String articleTable, String imageTable, String linkTable) { // schema 1
		// Note that to be able to match qrels with answers, the main table should be
		// the first relation and
		// the first attrib should be its ID
		Vector<Relation> relations = new Vector<Relation>();

		Relation rel = new Relation(articleTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("title", true, "VARCHAR2(256)");
		rel.addAttribute("text", true, "VARCHAR2(32000)");
		// rel.addAttribute("popularity", false, "INTEGER");
		rel.addAttr4Rel("id", "tbl_article_image_09");
		rel.addAttr4Rel("id", "tbl_article_link_09");
		rel.setSize(233909);
		relations.addElement(rel);

		rel = new Relation("tbl_article_image_09");
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttribute("image_id", false, "INTEGER");
		rel.addAttr4Rel("article_id", articleTable);
		rel.addAttr4Rel("image_id", imageTable);
		rel.setSize(3840433);
		relations.addElement(rel);

		rel = new Relation(imageTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("src", true, "VARCHAR(256)");
		rel.addAttr4Rel("id", "tbl_article_image_09");
		rel.setSize(1183070);
		relations.addElement(rel);

		rel = new Relation("tbl_article_link_09");
		rel.addAttribute("link_id", false, "INTEGER");
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttr4Rel("link_id", linkTable);
		rel.addAttr4Rel("article_id", articleTable);
		rel.setSize(120916125);
		relations.addElement(rel);

		rel = new Relation(linkTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("url", true, "VARCHAR(255)");
		rel.addAttr4Rel("id", "tbl_article_link_09");
		rel.setSize(9766351);
		relations.addElement(rel);

		return relations;
	}

}