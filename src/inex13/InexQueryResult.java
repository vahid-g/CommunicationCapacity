package inex13;

import java.util.ArrayList;

public class InexQueryResult {
	
	InexQueryDAO query;
	
	ArrayList<Integer> returnedDocs = new ArrayList<Integer>();
	
	public double precisionAtK(int k){
		double sum = 0.0;
		int l = k > returnedDocs.size() ? returnedDocs.size() : k;
		for (int i = 0; i < l; i++){
			if (query.relDocIds.contains(returnedDocs.get(i)))
					sum++;
		}
		return sum / k;
	}

}
