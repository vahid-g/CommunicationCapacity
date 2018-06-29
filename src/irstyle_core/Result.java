package irstyle_core;

import java.util.Comparator;

/**
 * <p>
 * Title:
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company:
 * </p>
 * 
 * @author unascribed
 * @version 1.0
 */

public class Result {

	String str;
	double score;

	public Result(String res, double sc) {
		str = res;
		score = sc;
	}

	public void print() {
		System.out.println(str.substring(0, 20) + " " + score);
	}

	public static class ResultComparator implements Comparator {
		public int compare(Object o1, Object o2) {
			if (((Result) o1).score > ((Result) o2).score)
				return -1;
			else if (((Result) o1).score < ((Result) o2).score)
				return 1;
			else
				return 0;
		}
	}
	
	public String getStr() {
		return str;
	}
}