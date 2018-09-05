package irstyle;

public class WikiConstants {

	public static String[] tableName = { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };

	public static String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };

	// best inex p20 sizes. p20 = 0.37 4%, 24%, 4%
	public static int[] precisionLimit = { 597251, 295765, 488317 };

	// best inex recall sizes wth v2 24%, 59%, 19%
	public static int[] recallLimit = { 2986255, 709836, 1953268};
	
	// best msn mrr sizes obtained with v2
	public static int[] mrrLimit = { 238900, 106470, 195326 };

	public static int[] size = { 11945034, 1183070, 9766351 };

	public final static String WIKI_DATA_DIR = "/data/ghadakcv/wikipedia/";
	
	public static String[] relationTable = {"tbl_article_image_09", "tbl_article_link_09"};
	
	public static String[] relationCacheTable = {"sub_article_image_09", "sub_article_link_09", };

}
