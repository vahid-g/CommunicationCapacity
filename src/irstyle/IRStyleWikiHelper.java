package irstyle;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import irstyle.api.DatabaseHelper;
import irstyle.api.IRStyleKeywordSearch;
import irstyle.core.JDBCaccess;
import irstyle.core.Relation;
import query.ExperimentQuery;

public class IRStyleWikiHelper {

	public static Vector<Relation> createRelations(String articleTable, String imageTable, String linkTable,
			String articleImageTable, String articleLinkTable, Connection conn) throws SQLException {
		// Note that to be able to match qrels with answers, the main table should be
		// the first relation and
		// the first attrib should be its ID
		Vector<Relation> relations = new Vector<Relation>();
		Relation rel = new Relation(articleTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("title", true, "VARCHAR2(256)");
		rel.addAttribute("text", true, "VARCHAR2(32000)");
		// rel.addAttribute("popularity", false, "INTEGER");
		rel.addAttr4Rel("id", articleImageTable);
		rel.addAttr4Rel("id", articleLinkTable);
		rel.setSize(DatabaseHelper.tableSize(articleTable, conn));
		relations.addElement(rel);
	
		rel = new Relation(articleImageTable);
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttribute("image_id", false, "INTEGER");
		rel.addAttr4Rel("article_id", articleTable);
		rel.addAttr4Rel("image_id", imageTable);
		rel.setSize(DatabaseHelper.tableSize(articleImageTable, conn));
		relations.addElement(rel);
	
		rel = new Relation(imageTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("src", true, "VARCHAR(256)");
		rel.addAttr4Rel("id", articleImageTable);
		rel.setSize(DatabaseHelper.tableSize(imageTable, conn));
		relations.addElement(rel);
	
		rel = new Relation(articleLinkTable);
		rel.addAttribute("link_id", false, "INTEGER");
		rel.addAttribute("article_id", false, "INTEGER");
		rel.addAttr4Rel("link_id", linkTable);
		rel.addAttr4Rel("article_id", articleTable);
		rel.setSize(DatabaseHelper.tableSize(articleLinkTable, conn));
		relations.addElement(rel);
	
		rel = new Relation(linkTable);
		rel.addAttribute("id", false, "INTEGER");
		rel.addAttribute("url", true, "VARCHAR(255)");
		rel.addAttr4Rel("id", articleLinkTable);
		rel.setSize(DatabaseHelper.tableSize(linkTable, conn));
		relations.addElement(rel);
	
		return relations;
	}

	public static JDBCaccess jdbcAccess() throws IOException {
		return IRStyleKeywordSearch.jdbcAccess("wikipedia");
	}

	public static Map<ExperimentQuery, Integer> buildQueryRelcountMap(Connection conn, List<ExperimentQuery> queryList)
			throws SQLException {
		Map<ExperimentQuery, Integer> map = new HashMap<ExperimentQuery, Integer>();
		for (ExperimentQuery query : queryList) {
			map.put(query, DatabaseHelper.relCountsForWiki(conn, query.getQrelScoreMap().keySet()));
		}
		return map;
	}

}
