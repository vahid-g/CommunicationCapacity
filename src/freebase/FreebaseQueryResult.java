package freebase;

public class FreebaseQueryResult {

    public FreebaseQueryResult(FreebaseQuery fq) {
	this.freebaseQuery = fq;
    }

    int relRank = -1;
    String[] top3Hits = new String[3];
    FreebaseQuery freebaseQuery;

    public double mrr() {
	if (relRank != -1)
	    return 1.0 / relRank;
	else
	    return 0;
    }

    public double p3() {
	if (relRank != -1 && relRank < 4)
	    return 0.3;
	else
	    return 0;
    }

    public double precisionAtK(int k) {
	if (relRank != -1 && relRank <= k) {
	    return 1.0 / k;
	} else {
	    return 0;
	}
    }

    @Override
    public String toString() {
	return freebaseQuery.id + ", " + freebaseQuery.text + ", "
		+ freebaseQuery.frequency;
    }

}
