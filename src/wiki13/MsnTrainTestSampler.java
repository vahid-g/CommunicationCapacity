package wiki13;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import query.ExperimentQuery;
import query.QueryServices;


// Builds a train and test set for msn queries by sampling them 
// Note that we sample without replacement
public class MsnTrainTestSampler {

	final static Logger LOGGER = Logger.getLogger(MsnTrainTestSampler.class.getName());

	public static void main(String[] args) throws IOException {
		List<ExperimentQuery> queries = QueryServices.loadMsnQueriesWithFreqs(
				"/nfs/stak/users/ghadakcv/workspace/queries/msn_all.csv",
				"/nfs/stak/users/ghadakcv/workspace/queries/msn.qrels");
		int sum = 0;
		int[] freqs = new int[queries.size()];
		for (ExperimentQuery query : queries) {
			freqs[0] = query.getFreq();
			sum += query.getFreq();
		}
		int sampleSize = (int) (sum * 0.1);
		int[] train = new int[queries.size()];
		Random rand = new Random();
		for (int i = 0; i < sampleSize; i++) {
			int r = rand.nextInt(sum);
			for (int j = 0; j < freqs.length; j++) {
				if (r < freqs[j]) {
					train[j]++;
					freqs[j]--;
					sum--;
					break;
				}
				r = r - freqs[j];
			}
		}
		LOGGER.log(Level.INFO, "sampling done!");
		try (FileWriter fwTrain = new FileWriter("data/wiki/msn_train.csv"); 
				FileWriter fwTest = new FileWriter("data/wiki/msn_test.csv")) {
			for (int i = 0; i < queries.size(); i++) {
				ExperimentQuery query = queries.get(i);
				fwTrain.write(query.getId() + "," + query.getText() + "," + train[i] + "\n");
				fwTest.write(query.getId() + "," + query.getText() + "," + freqs[i] + "\n");
			}
		}
	}
}
