package stackoverflow;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import database.DatabaseConnection;
import database.DatabaseType;

// This class splits the view counts into train/test set by doing sampling without replacement
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

		int sum = 0;
		for (int i = 1; i < viewcounts.length; i++) {
			sum += viewcounts[i];
		}

		// sample
		int sampleSize = viewcounts.length / 2;
		int[] train = new int[QUESTIONS_A_SIZE];
		Random random = new Random();
		for (int i = 0; i < sampleSize; i++) {
			int r = random.nextInt(sum);
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
		
		int[] test = new int[QUESTIONS_A_SIZE];
		for (int i = 0; i < train.length; i++) {
			test[i] = viewcounts[i] - train[i];
		}

		try (FileWriter fwTrain = new FileWriter("id_viewcount_train");
				FileWriter fwTest = new FileWriter("id_viewcount_test")) {
			for (int i = 0; i < QUESTIONS_A_SIZE; i++) {
				fwTrain.write(ids[i] + "," + train[i] + "\n");
				fwTest.write(ids[i] + "," + test[i] + "\n");
			}
		}
	}
}
