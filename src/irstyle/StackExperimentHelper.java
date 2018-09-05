package irstyle;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;

import irstyle.api.DatabaseHelper;
import irstyle.api.IRStyleExperimentHelper;
import irstyle.api.IRStyleKeywordSearch;
import irstyle.core.Relation;

public class StackExperimentHelper extends IRStyleExperimentHelper {

	public StackExperimentHelper() throws IOException {
		jdbcAccess = IRStyleKeywordSearch.jdbcAccess("stack_overflow");
	}

	public Vector<Relation> createRelations(String answersTable, String tagsTable, String commentsTable,
			String answerTagsTable, String answerCommentsTable) throws SQLException {
		// Note that to be able to match qrels with answers, the main table should be
		// the first relation and the first attrib should be its ID
		Connection conn = jdbcAccess.conn;
		Vector<Relation> relations = new Vector<Relation>();
		Relation rel = new Relation(answersTable);
		rel.addAttribute("Id", false, "INTEGER");
		rel.addAttribute("Body", true, "TEXT");
		rel.addAttr4Rel("Id", answerTagsTable);
		rel.addAttr4Rel("Id", answerCommentsTable);
		rel.setSize(DatabaseHelper.tableSize(answersTable, conn));
		relations.addElement(rel);

		rel = new Relation(answerTagsTable);
		rel.addAttribute("AnswerId", false, "INTEGER");
		rel.addAttribute("TagId", false, "INTEGER");
		rel.addAttr4Rel("AnswerId", answersTable);
		rel.addAttr4Rel("TagId", tagsTable);
		relations.addElement(rel);

		rel = new Relation(tagsTable);
		rel.addAttribute("Id", false, "INTEGER");
		rel.addAttribute("TagName", true, "TEXT");
		rel.addAttr4Rel("Id", answerTagsTable);
		rel.setSize(DatabaseHelper.tableSize(tagsTable, conn));
		relations.addElement(rel);

		rel = new Relation(answerCommentsTable);
		rel.addAttribute("AnswerId", false, "INTEGER");
		rel.addAttribute("CommentId", false, "INTEGER");
		rel.addAttr4Rel("AnswerId", answersTable);
		rel.addAttr4Rel("CommentId", commentsTable);
		relations.addElement(rel);

		rel = new Relation(commentsTable);
		rel.addAttribute("Id", false, "INTEGER");
		rel.addAttribute("Text", true, "TEXT");
		rel.addAttr4Rel("Id", answerCommentsTable);
		rel.setSize(DatabaseHelper.tableSize(commentsTable, conn));
		relations.addElement(rel);

		return relations;
	}
}
