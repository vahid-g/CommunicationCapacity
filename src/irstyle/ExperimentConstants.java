package irstyle;

public class ExperimentConstants {

	public static String[] tableName = { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };

	public static String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };

	// best inex p20 sizes. p20 = 0.37 4%, 24%, 4%
	static int[] precisionLimit = { 597251, 295765, 488317 };

	// best inex recall sizes wth v2 3%, 16%, 55%
	//static int[] recallLimit = { 400000, 200000, 500000 };
	static int[] recallLimit = { 597251, 414071, 976634 };
	
	// best msn mrr sizes obtained with v2
	static int[] mrrLimit = { 238900, 106470, 195326 };

	static int[] size = { 11945034, 1183070, 9766351 };

	final static String WIKI_DATA_DIR = "/data/ghadakcv/wikipedia/";

}
