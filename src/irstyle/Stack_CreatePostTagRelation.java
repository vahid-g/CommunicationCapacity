package irstyle;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import database.DatabaseConnection;
import database.DatabaseType;

public class Stack_CreatePostTagRelation {

	public static void main(String[] args) throws IOException, SQLException {
		try (DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW)) {
			Map<String, Integer> tagIdMap = new HashMap<String, Integer>();
			String selectTag = "select Id,TagName from abtin.Tags;";
			try (Statement stmt = dc.getConnection().createStatement()) {
				ResultSet rs = stmt.executeQuery(selectTag);
				while (rs.next()) {
					String tag = rs.getString("TagName");
					Integer id = rs.getInt("Id");
					tagIdMap.put(tag, id); // assuming id-tags are unique
				}
			}
			String selectPosts = "select Id, Tags from abtin.Posts";
			String insert = "insert into abtin.PostTags (PostId, TagId) values (?,?);";
			PreparedStatement preparedInsert = dc.getConnection().prepareStatement(insert);
			try (Statement stmt = dc.getConnection().createStatement()) {
				ResultSet rs = stmt.executeQuery(selectPosts);
				while (rs.next()) {
					int postId = rs.getInt("Id");
					String tagsString = rs.getString("Tags");
					if (tagsString == null) {
						continue;
					}
					String[] tags = tagsString.split("[>|<]");
					for (String tag : tags) {
						if (tag.trim() == "") {
							continue;
						}
						Integer tagId = tagIdMap.get(tag);
						if (tagId == null) {
							System.out.println("Couldn't find tag: '" + tag + "' in Tags table");
							continue;
						} else {
							preparedInsert.setInt(1, postId);
							preparedInsert.setInt(2, tagId);
							preparedInsert.addBatch();
						}
					}
				}
				preparedInsert.executeBatch();
			}
			System.out.println("Success!");
		}
	}

}
