package irstyle;

public class ExperimentConstants {

	public static String[] tableName = { "tbl_article_wiki13", "tbl_image_pop", "tbl_link_pop" };

	public static String[][] textAttribs = new String[][] { { "title", "text" }, { "src" }, { "url" } };

	// best inex p20 sizes with v2 1%, 8%, 1%
	static int[] precisionLimit = { 119450, 94640, 97663 };

	// best inex recall sizes wth v2 3%, 16%, 55
	static int[] recallLimit = { 400000, 200000, 500000 };

	// best msn mrr sizes obtained with v2
	static int[] mrrLimit = { 238900, 106470, 195326 };

	static int[] size = { 11945034, 1183070, 9766351 };

	final static String MAPLE_DATA_DIR = "/data/ghadakcv/wikipedia/";

}
