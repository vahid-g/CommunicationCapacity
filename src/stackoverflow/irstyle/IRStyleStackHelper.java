package stackoverflow.irstyle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;

import irstyle.api.DatabaseHelper;
import irstyle.core.Relation;

public class IRStyleStackHelper {
	public static Vector<Relation> createRelations(String answersTable, String postTagsTable, String tagsTable,
			String postCommentsTable, String commentsTable, Connection conn) throws SQLException {
		// Note that to be able to match qrels with answers, the main table should be
		// the first relation and the first attrib should be its ID
		Vector<Relation> relations = new Vector<Relation>();
		Relation rel = new Relation(answersTable);
		rel.addAttribute("Id", false, "INTEGER");
		rel.addAttribute("ParentId", false, "INTEGER");
		rel.addAttribute("Body", true, "TEXT");
		rel.addAttr4Rel("ParentId", postTagsTable);
		rel.addAttr4Rel("Id", postCommentsTable);
		rel.setSize(DatabaseHelper.tableSize(answersTable, conn));
		relations.addElement(rel);

		rel = new Relation(postTagsTable);
		rel.addAttribute("PostId", false, "INTEGER");
		rel.addAttribute("TagId", false, "INTEGER");
		rel.addAttr4Rel("PostId", answersTable);
		rel.addAttr4Rel("TagId", tagsTable);
		relations.addElement(rel);

		rel = new Relation(tagsTable);
		rel.addAttribute("Id", false, "INTEGER");
		rel.addAttribute("TagName", true, "TEXT");
		rel.addAttr4Rel("Id", postTagsTable);
		rel.setSize(DatabaseHelper.tableSize(tagsTable, conn));
		relations.addElement(rel);

		rel = new Relation(postCommentsTable);
		rel.addAttribute("CommentId", false, "INTEGER");
		rel.addAttribute("PostId", false, "INTEGER");
		rel.addAttr4Rel("CommentId", commentsTable);
		rel.addAttr4Rel("PostId", answersTable);
		relations.addElement(rel);

		rel = new Relation(commentsTable);
		rel.addAttribute("Id", false, "INTEGER");
		rel.addAttribute("PostId", false, "INTEGER");
		rel.addAttribute("Text", true, "TEXT");
		rel.addAttr4Rel("PostId", postCommentsTable);
		rel.setSize(DatabaseHelper.tableSize(commentsTable, conn));
		relations.addElement(rel);

		return relations;
	}
}
