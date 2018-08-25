package irstyle.api;

public class Params {

	public static int MAX_GENERATED_CN = 50;

	public static int MAX_ALLOWED_TIME = 2 * 60 * 1000;

	public static boolean DEBUG = false;

	public static int maxCNsize = 5;

	public static int numExecutions = 1;

	public static int N = 20;

	public static boolean allKeywInResults = false;

	public static int MAX_TS_SIZE = 1000;

	public static String getDescriptor() {
		StringBuilder sb = new StringBuilder();
		sb.append("MAX_ALLOWED_TIME = " + MAX_ALLOWED_TIME + "\n");
		sb.append("N = " + N + "\n");
		sb.append("MAX_TS_SIZE = " + MAX_TS_SIZE + "\n");
		sb.append("Score thresholding = " + useScoreThresholding + "\n");
		return sb.toString();
	}

	public static boolean useScoreThresholding = false;

}
