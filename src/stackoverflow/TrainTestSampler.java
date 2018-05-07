package stackoverflow;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

import database.DatabaseConnection;
import database.DatabaseType;

// This class splits the view counts into train/test set by doing sampling without replacement
public class TrainTestSampler {

	final static int QUESTIONS_A_SIZE = 8034000;
	final static int QUESTIONS_S_SIZE = 1092420;

	private static Logger LOGGER = Logger.getLogger(TrainTestSampler.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		String tableName = args[0]; //"questions_a"
		int tableSize = Integer.parseInt(args[1]); //QUESTIONS_A_SIZE
		sampleWithReplacecment(tableName, tableSize);
	}

	static void sampleWithReplacecment(String tableName, int tableSize) throws IOException, SQLException {
		// read the ids and viewcounts
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		int[] ids = new int[tableSize];
		int[] viewcounts = new int[tableSize];
		try (Statement stmt = conn.createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stmt.executeQuery("select Id, ViewCount from " + tableName + " order by ViewCount desc");
			int i = 0;
			while (rs.next()) {
				int id = Integer.parseInt(rs.getString("Id"));
				int viewcount = Integer.parseInt(rs.getString("ViewCount"));
				ids[i] = id;
				viewcounts[i] = viewcount;
				i++;
			}
		}
		dc.closeConnection();
		double sum = 0.0;
		for (int i = 0; i < tableSize; i++) {
			sum += viewcounts[i];
		}
		LOGGER.log(Level.INFO, "sum = " + sum);
		double[] cdf = new double[tableSize];
		cdf[0] = viewcounts[0] / sum;
		for (int i = 1; i < tableSize; i++) {
			cdf[i] = cdf[i - 1] + (viewcounts[i] / sum);
		}
		LOGGER.log(Level.INFO, "cdf is built. last element: " + cdf[tableSize - 1]);
		double sampleSize = sum / 2;
		int[] train = new int[tableSize];
		Random rand = new Random();
		for (long i = 0; i < sampleSize; i++) {
			if (i % 100000000 == 0) {
				LOGGER.log(Level.INFO, i + "");
			}
			int k = Arrays.binarySearch(cdf, rand.nextDouble());
			k = k >= 0 ? k : (-k - 1);
			train[k]++;
		}
		LOGGER.log(Level.INFO, "sampling done!");
		try (FileWriter fwTrain = new FileWriter(tableName + "_train");
				FileWriter fwTest = new FileWriter(tableName + "_test")) {
			for (int i = 0; i < tableSize; i++) {
				fwTrain.write(ids[i] + "," + train[i] + "\n");
				fwTest.write(ids[i] + "," + Math.max(viewcounts[i] - train[i], 0) + "\n");
			}
		}

	}

	static void sampleWithoutReplacement(String tableName, int tableSize) throws IOException, SQLException {
		// read the ids and viewcounts
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		int[] ids = new int[tableSize];
		int[] viewcounts = new int[tableSize];
		try (Statement stmt = conn.createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stmt.executeQuery("select Id, ViewCount from " + tableName + " order by ViewCount desc");
			int i = 0;
			while (rs.next()) {
				int id = Integer.parseInt(rs.getString("Id"));
				int viewcount = Integer.parseInt(rs.getString("ViewCount"));
				ids[i] = id;
				viewcounts[i] = viewcount;
				i++;
			}
		}
		dc.closeConnection();

		long sum = 0;
		for (int i = 1; i < viewcounts.length; i++) {
			sum += viewcounts[i];
		}
		System.out.println("sum = " + sum);
		// sample
		long sampleSize = sum / 2;
		int[] train = new int[tableSize];
		for (int i = 0; i < sampleSize; i++) {
			long r = ThreadLocalRandom.current().nextLong(sum);
			for (int j = 0; j < viewcounts.length; j++) {
				if (r < viewcounts[j]) {
					train[j]++;
					viewcounts[j]--;
					sum--;
					break;
				}
				r = r - viewcounts[j];
			}
		}

		try (FileWriter fwTrain = new FileWriter("id_viewcount_train");
				FileWriter fwTest = new FileWriter("id_viewcount_test")) {
			for (int i = 0; i < tableSize; i++) {
				fwTrain.write(ids[i] + "," + train[i] + "\n");
				fwTest.write(ids[i] + "," + viewcounts[i] + "\n");
			}
		}
	}
}
