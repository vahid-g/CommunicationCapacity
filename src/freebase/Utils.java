package freebase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	Map<FreebaseQuery, Integer> freqMap = new HashMap<FreebaseQuery, Integer>();
	for (FreebaseQuery query : queries) {
	    if (freqMap.containsKey(query))
		freqMap.put(query, freqMap.get(query) + 1);
	    else
		freqMap.put(query, 1);
	}
	double[] pdf = getPdf(freqMap, queries);
	double[] cdf = getCdf(pdf);
	Random rand = new Random();
	List<FreebaseQuery> sampledQueries = new ArrayList<FreebaseQuery>();
	while (sampledQueries.size() < n) {
	    double r = rand.nextDouble();
	    int index = 0;
	    while (r > cdf[index] && pdf[index] > 0)
		index++;
	    FreebaseQuery query = queries.get(index);
	    sampledQueries.add(query);
	    int freq = Math.max(freqMap.get(query) - 1, 0);
	    freqMap.put(query, freq);
	    pdf = getPdf(freqMap, queries);
	    cdf = getCdf(pdf);
	}
	return sampledQueries;
    }

    private static double[] getCdf(double[] pdf) {
	double[] cdf = new double[pdf.length];
	double sum = 0;
	for (int i = 0; i < pdf.length; i++) {
	    cdf[i] = pdf[i] + sum;
	    sum = cdf[i];
	}
	return cdf;
    }

    private static double[] getPdf(Map<FreebaseQuery, Integer> freqMap,
	    List<FreebaseQuery> queries) {
	double[] pdf = new double[queries.size()];
	double sum = 0;
	for (Integer freq : freqMap.values())
	    sum += freq;
	for (int i = 0; i < queries.size(); i++) {
	    pdf[i] = freqMap.get(queries.get(i)) / sum;
	}
	return pdf;
    }

    public static List<FreebaseQuery> flattenFreebaseQueries(
	    List<FreebaseQuery> queries) {
	List<FreebaseQuery> flatList = new ArrayList<FreebaseQuery>();
	for (FreebaseQuery query : queries){
	    for (int i = 0; i < query.frequency; i++){
		flatList.add(new FreebaseQuery(i, query));
	    }
	}
	return flatList;
    }
    
}
