package irstyle.textsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import irstyle.IRStyleKeywordSearch;
import irstyle.IRStyleQueryResult;
import irstyle.api.Params;
import irstyle.core.JDBCaccess;
import irstyle.core.MIndexAccess;
import irstyle.core.Relation;
import irstyle.core.Result;
import irstyle.core.Schema;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiFilesPaths;

public class RunBaseline_MySQL {

	public static void main(String[] args) throws Exception {
		JDBCaccess jdbcacc = IRStyleKeywordSearch.jdbcAccess();
		for (int exec = 0; exec < Params.numExecutions; exec++) {
			String articleTable = "tbl_article_09";
			String imageTable = "tbl_image_09_tk";
			String linkTable = "tbl_link_09";
			String articleImageTable = "tbl_article_image_09";
			String articleLinkTable = "tbl_article_link_09";
			String schemaDescription = "5 " + articleTable + " " + articleImageTable + " " + imageTable + " "
					+ articleLinkTable + " " + linkTable + " " + articleTable + " " + articleImageTable + " "
					+ articleImageTable + " " + imageTable + " " + articleTable + " " + articleLinkTable + " "
					+ articleLinkTable + " " + linkTable;
			Vector<Relation> relations = IRStyleKeywordSearch.createRelations(articleTable, imageTable, linkTable,
					articleImageTable, articleLinkTable, jdbcacc.conn);
			IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
			WikiFilesPaths paths = null;
			paths = WikiFilesPaths.getMaplePaths();
			List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
					paths.getMsnQrelFilePath());
			Collections.shuffle(queries, new Random(1));
			queries = queries.subList(0, 50);
			List<IRStyleQueryResult> queryResults = new ArrayList<IRStyleQueryResult>();
			int loop = 1;
			for (ExperimentQuery query : queries) {
				System.out.println("processing query " + loop++ + "/" + queries.size() + ": " + query.getText());
				Schema sch = new Schema(schemaDescription);
				IRStyleQueryResult result = executeIRStyleQuery(jdbcacc, sch, relations, query);
				queryResults.add(result);
			}
			IRStyleKeywordSearch.printRrankResults(queryResults, "ir_result.csv");
		}
	}

	static IRStyleQueryResult executeIRStyleQuery(JDBCaccess jdbcacc, Schema sch, Vector<Relation> relations,
			ExperimentQuery query) {
		MIndexAccess MIndx = new MIndexAccess(relations);
		Vector<String> allkeyw = new Vector<String>();
		// escaping single quotes
		allkeyw.addAll(Arrays.asList(query.getText().replace("'", "\\'").split(" ")));
		int exectime = 0;
		long time3 = System.currentTimeMillis();
		MIndx.createTupleSets2(sch, allkeyw, jdbcacc.conn);
		long time4 = System.currentTimeMillis();
		exectime += time4 - time3;
		System.out.println(" Time to create tuple sets: " + (time4 - time3) + " (ms)");
		time3 = System.currentTimeMillis();
		/** returns a vector of instances (tuple sets) */ // P1
		Vector<?> CNs = sch.getCNs(Params.maxCNsize, allkeyw, sch, MIndx);
		for (Object v : CNs) {
			System.out.println(v);
		}
		time4 = System.currentTimeMillis();
		exectime += time4 - time3;
		System.out.println(" #CNs=" + CNs.size() + " Time to get CNs=" + (time4 - time3) + " (ms)");
		ArrayList<Result> results = new ArrayList<Result>(1);
		exectime += IRStyleKeywordSearch.methodC(Params.N, Params.allKeywInResults, relations, allkeyw, CNs, results,
				jdbcacc);
		IRStyleKeywordSearch.dropTupleSets(jdbcacc, relations);
		IRStyleQueryResult result = new IRStyleQueryResult(query, exectime);
		result.addIRStyleResults(results);
		System.out.println(" R-rank = " + result.rrank());
		return result;
	}

}
