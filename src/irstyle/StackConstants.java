package irstyle;

public class StackConstants {

	public static String[] tableName = { "answers_s_train", "tags_pop", "comments_pop" };

	public static int[] size = { 1092420, 1092420, 1967107 };

	// 0.25, 0.2 and 0.1 percent
	public static int[] cacheSize = { 273105, 218484, 196710 };

	public static String[][] textAttribs = new String[][] { { "Body" }, { "TagName" }, { "Text" } };

	public static final String ANSWER_TAGS_TABLE = "answer_tags";

	public static final String ANSWER_COMMENTS_TABLE = "answer_comments";

	public static final String DATA_STACK = "/data/ghadakcv/stack/";

	public static final String[] relationTables = { "answer_tags", "answer_comments" };

	public static final String[] relationCacheTables = { "sub_answer_tags", "sub_answer_comments" };

}
