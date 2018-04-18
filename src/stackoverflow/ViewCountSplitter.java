package stackoverflow;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

import database.DatabaseConnection;
import database.DatabaseType;

public class ViewCountSplitter {

	final static int QUESTIONS_A_SIZE = 8034000;

	public static void main(String[] args) throws IOException, SQLException {
		// read the ids and viewcounts
		DatabaseConnection dc = new DatabaseConnection(DatabaseType.STACKOVERFLOW);
		Connection conn = dc.getConnection();
		int[] ids = new int[QUESTIONS_A_SIZE];
		int[] viewcounts = new int[QUESTIONS_A_SIZE];
		try (Statement stmt = conn.createStatement()) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			ResultSet rs = stmt.executeQuery("select Id, ViewCount from questions_a order by ViewCount desc");
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
		
		// build cdf
		int[] cumulative = new int[QUESTIONS_A_SIZE];
		cumulative[0] = viewcounts[0];
		for (int i = 1; i < QUESTIONS_A_SIZE; i++) {
			cumulative[i] = cumulative[i - 1] + viewcounts[i];
		}

		// sample
		int sampleSize = QUESTIONS_A_SIZE / 2;
		int max = cumulative[QUESTIONS_A_SIZE - 1];
		int[] train = new int[QUESTIONS_A_SIZE];
		Random random = new Random();
		for (int i = 0; i < sampleSize; i++) {
			int k = Arrays.binarySearch(cumulative, random.nextInt(max));
			k = k >= 0 ? k : (-k - 1);
			train[k]++;
		}
		int[] test = new int[QUESTIONS_A_SIZE];
		for (int i = 0; i < sampleSize; i++) {
			test[i] = viewcounts[i] - train[i];
		}

		try (FileWriter fwTrain = new FileWriter("id_viewcount_trian");
				FileWriter fwTest = new FileWriter("id_viewcount_test")) {
			for (int i = 0; i < QUESTIONS_A_SIZE; i++) {
				fwTrain.write(ids[i] + "," + train[i] + "\n");
				fwTest.write(ids[i] + "," + test[i] + "\n");
			}
		}
	}
}
