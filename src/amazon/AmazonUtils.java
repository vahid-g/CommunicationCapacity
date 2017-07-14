package amazon;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AmazonUtils {

	static final Logger LOGGER = Logger.getLogger(AmazonUtils.class.getName());

	public static void main(String[] args) {

		try (FileWriter fw = new FileWriter("data/amazon_path_rate.csv");
				FileReader fr = new FileReader("data/ratings_Books.csv")) {
			parseRatings(fr, fw);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public static void parseRatings(Reader reader, Writer writer) {
		try (BufferedReader br = new BufferedReader(reader)) {
			String line = br.readLine();
			String lastIsbn = "0000000000";
			double sum = 0;
			double count = 0;
			Pattern ptr = Pattern.compile(".+,([0-9X]{10}),([0-9.]+),\\d+");
			while (line != null) {
				Matcher matcher = ptr.matcher(line);
				if (matcher.find()) {
					String isbn = matcher.group(1);
					Double rate = Double.parseDouble(matcher.group(2));
					if (lastIsbn.equals(isbn)) {
						sum += rate;
						count++;
					} else {
						String folder = lastIsbn.substring(lastIsbn.length() - 3);
						if (count != 0) { // this if is to skip the first line
							double totalRate = sum / count;
							writer.write(folder + "/" + lastIsbn + ".xml," + totalRate + "\n");
						}
						sum = rate;
						count = 1;
					}
					lastIsbn = isbn;
				} else {
					LOGGER.log(Level.SEVERE, "Couldn't parse line: " + line);
					break;
				}
				line = br.readLine();
			}
			String folder = lastIsbn.substring(lastIsbn.length() - 3);
			double totalRate = sum / count;
			writer.write(folder + "/" + lastIsbn + ".xml," + totalRate);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}
}
