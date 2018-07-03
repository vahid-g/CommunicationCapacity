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
		JDBCaccess jdbcacc = SimpleMainRefactored.jdbcAccess();
		WikiFilesPaths paths = null;
		paths = WikiFilesPaths.getMaplePaths();
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
				paths.getMsnQrelFilePath());
		SimpleMainRefactored.dropTupleSets(jdbcacc);
		FeatureExtraction fe = new FeatureExtraction(WikiFileIndexer.WEIGHT_ATTRIB);
		try (FileWriter fw = new FileWriter("result.csv")) {
			int loop = 1;
			for (ExperimentQuery query : queries) {
				Vector<String> allkeyw = new Vector<String>();
				allkeyw.addAll(Arrays.asList(query.getText().split(" ")));
				// allkeyw.add("jimmy");
				// allkeyw.add("hoffa");
				System.out.println("processing " + allkeyw + " " + ((100 * loop++) / queries.size()) + "% completed");

				try (IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get("index_path")));
						IndexReader globalIndexReader = DirectoryReader
								.open(FSDirectory.open(Paths.get("global_index_path")));
						IndexReader restIndexReader = DirectoryReader
								.open(FSDirectory.open(Paths.get("rest_index_path")))) {
					double ql_cache = fe.queryLikelihood(indexReader, query.getText(), "field", globalIndexReader,
							new StandardAnalyzer());
					double ql_rest = fe.queryLikelihood(globalIndexReader, query.getText(), "field", globalIndexReader,
							new StandardAnalyzer());
				}

				String articleTable = "tbl_article_09";
				String imageTable = "tbl_image_09_tk";
				String linkTable = "tbl_link_09";
				String articleImageTable = "tbl_article_image_09";
				String articleLinkTable = "tbl_article_link_09";
				String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
						+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
						+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
						+ articleLinkTable + " " + linkTable;
				Schema sch = new Schema(schemaDescription);
				Vector<Relation> relations = SimpleMainRefactored.createRelations();

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
					exectime = SimpleMainRefactored.methodB(N, allKeywInResults, relations, allkeyw, CNs, results,
							jdbcacc);
				} else {
					exectime = SimpleMainRefactored.methodC(N, allKeywInResults, relations, allkeyw, CNs, results,
							jdbcacc);
				}
				SimpleMainRefactored.dropTupleSets(jdbcacc);
				double mrr = SimpleMainRefactored.mrr(results, query);
				System.out.println(" R-rank = " + mrr);
				fw.write(
						query.getId() + "," + query.getText().replaceAll(",", " ") + "," + mrr + "," + exectime + "\n");
			}
		}
	}

}
