package freebase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Utils {

	static void shuffleArray(Object[] ar) {
		// If running on Java 6 or older, use `new Random()` on RHS here
		Random rnd = new Random();
		for (int i = ar.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			Object a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}

	public static int[][] addMatrix(int[][] a, int[][] b) {
		int[][] c = new int[a.length][a[0].length];
		for (int i = 0; i < a.length; i++) {
			for (int j = 0; j < a[0].length; j++) {
				c[i][j] = a[i][j] + b[i][j];
			}
		}
		return c;
	}

	public static List<FreebaseQuery> sampleFreebaseQueries(
			List<FreebaseQuery> queries, int n) {
		double[] pdf = new double[queries.size()];
		double sum = 0;
		for (FreebaseQuery query : queries) {
			sum += query.frequency;
		}
		for (int i = 0; i < queries.size(); i++) {
			pdf[i] = queries.get(i).frequency / sum;
		}
		double[] cdf = new double[queries.size()];
		sum = 0;
		for (int i = 0; i < queries.size(); i++) {
			cdf[i] = pdf[i] + sum;
			sum = cdf[i];
		}
		Random rand = new Random();
		List<FreebaseQuery> sampledQueries = new ArrayList<FreebaseQuery>();
		while (sampledQueries.size() < n){
			double r = rand.nextDouble();
			int index = 0;
			while (r > cdf[index])
				index++;
			if (sampledQueries.contains(queries.get(index))){
				continue;
			} else {
				sampledQueries.add(queries.get(index));
			}
		}
		return sampledQueries;
	}

}
