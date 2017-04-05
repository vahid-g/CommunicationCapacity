package wiki_inex09;

import java.util.ArrayList;
import java.util.List;

public class MsnQueryResult {

	public MsnQueryResult(MsnQuery msnQuery) {
		this.msnQuery = msnQuery;
	}

	MsnQuery msnQuery;
	public int rank = -1;
	public List<String> results = new ArrayList<String>();

	public double mrr() {
		if (rank != -1)
			return 1.0 / rank;
		else
			return 0;
	}

	public double precisionAtK(int k) {
		if (rank != -1 && rank <= k) {
			return 1.0 / k;
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return "\"" + msnQuery.text.replace(",", " ").replace("\"", " ") + "\", " + precisionAtK(3) + ", " + mrr();
	}

	public String fullResult() {
		StringBuilder sb = new StringBuilder();
		for (String result : results) {
			sb.append(result + ", ");
		}
		String resultTuples = sb.toString();
		if (resultTuples.length() > 2)
			resultTuples = resultTuples.substring(0, resultTuples.length() - 2);
		return this.toString() + ", " + resultTuples;
	}

}
