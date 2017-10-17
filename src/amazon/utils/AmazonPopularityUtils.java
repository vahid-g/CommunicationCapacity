package amazon.utils;

import indexing.InexFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import wiki_inex09.Utils;
import amazon.AmazonIsbnConverter;

public class AmazonPopularityUtils {

	static final Logger LOGGER = Logger.getLogger(AmazonPopularityUtils.class
			.getName());

	static int missingValues = 0;

	public static void main(String[] args) {
		// buildSortedPathReviewsList(AmazonDirectoryInfo.DATA_SET);
		// buildPathRatingsList();
		// buildPathReviewRelScoreList();
		// buildPathReviewRateList();
		buildPathReviewsRatesList("/data/amazon/amazon-inex/", "/data/ghadakcv/amazon_path_reviews_ratings.csv");
	}

	public static List<InexFile> buildPathReviewsRatesList(String datasetPath,
			String filesListPath) {
		List<InexFile> pathCount = new ArrayList<InexFile>();
		List<String> filePaths = Utils
				.listFilesForFolder(new File(datasetPath));
		try (FileWriter fw = new FileWriter(filesListPath)) {
			for (String filepath : filePaths) {
				if (filepath.contains(".dtd"))
					continue;
				DocumentBuilderFactory dbf = DocumentBuilderFactory
						.newInstance();
				try {
					DocumentBuilder db = dbf.newDocumentBuilder();
					org.w3c.dom.Document doc = db.parse(new File(filepath));
					NodeList reviewNodeList = doc
							.getElementsByTagName("review");
					NodeList ratingNodeList = doc
							.getElementsByTagName("rating");
					double ratingAvg = 0;
					int ratingCount = ratingNodeList.getLength();
					for (int i = 0; i < ratingNodeList.getLength(); i++) {
						Node node = ratingNodeList.item(i);
						ratingAvg += Double.parseDouble(node.getTextContent());
					}
					if (ratingCount > 0) ratingAvg = Math.floor((ratingAvg / ratingCount) * 100.0) / 100.0;
					fw.write(filepath + "," + reviewNodeList.getLength() + ","
							+ ratingAvg + "," + ratingCount + "\n");
				} catch (Exception e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (pathCount);
	}
	
	public static List<InexFile> buildSortedPathReviewsList(String datasetPath,
			String filesListPath) {
		List<InexFile> pathCount = new ArrayList<InexFile>();
		List<String> filePaths = Utils
				.listFilesForFolder(new File(datasetPath));
		for (String filepath : filePaths) {
			if (filepath.contains(".dtd"))
				continue;
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			try {
				DocumentBuilder db = dbf.newDocumentBuilder();
				org.w3c.dom.Document doc = db.parse(new File(filepath));
				NodeList reviewNodeList = doc.getElementsByTagName("review");
				pathCount
						.add(new InexFile(filepath, reviewNodeList.getLength()));
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		Collections.sort(pathCount, new InexFile.ReverseWeightComparator());
		try (FileWriter fw = new FileWriter(filesListPath)) {
			for (InexFile dfm : pathCount) {
				fw.write(dfm.path + "," + dfm.weight + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (pathCount);
	}

	public static void buildPathRatingsList() {
		try (FileWriter fw = new FileWriter("data/amazon_path_rate.csv");
				FileReader fr = new FileReader("data/ratings_Books.csv")) {
			parseUcsdIsbnRatingsData(fr, fw);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	static void parseUcsdIsbnRatingsData(Reader reader, Writer writer) {
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
						String folder = lastIsbn
								.substring(lastIsbn.length() - 3);
						if (count != 0) { // this if is to skip the first line
							double totalRate = sum / count;
							writer.write(folder + "/" + lastIsbn + ".xml,"
									+ totalRate + "," + count + "\n");
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

	public static void buildPathReviewRelScoreList() {
		try (FileWriter fw = new FileWriter(
				"data/amazon_data/popularity/ap4r.csv")) {
			Map<String, Integer> ltidScoresMap = loadLtidTotalScoreMap("data/amazon_data/inex14sbs.qrels");
			Map<String, String> isbnLtidMap = AmazonIsbnConverter
					.loadIsbnLtidMap("data/amazon_data/amazon-lt.isbn.thingID.csv");
			parsePathReviewsRels("data/amazon_data/popularity/ap3r.csv",
					ltidScoresMap, isbnLtidMap, fw);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private static void parsePathReviewsRels(String pathRevPath,
			Map<String, Integer> ltidScoresMap,
			Map<String, String> isbnLtidMap, Writer writer) {
		LOGGER.log(Level.INFO, "Building <Path, #Reviews, RelScore> file..");
		try {
			for (String line : Files.readAllLines(Paths.get(pathRevPath))) {
				String[] fields = line.split(",");
				String path = fields[0];
				String isbn = path.substring(path.lastIndexOf('/') + 1,
						path.lastIndexOf('.'));
				String ltid = isbnLtidMap.get(isbn);
				Integer score = 0;
				if (ltidScoresMap.containsKey(ltid)) {
					score = ltidScoresMap.get(ltid);
				} else {
					missingValues++;
				}
				writer.write(line + "," + score + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO, "Missed value count: " + missingValues);
	}

	static void buildPathReviewRateList() {
		try (FileWriter fw = new FileWriter("data/amazon_path_reviews_rate.csv")) {
			Map<String, String> isbnRateMap = AmazonPopularityUtils
					.loadIsbnRatingsMap("data/amazon_path_rate.csv");
			LOGGER.log(Level.SEVERE,
					"Size of IsbnRatings map: " + isbnRateMap.size());
			parsePathReviewsRates("data/amazon_path_reviews.csv", isbnRateMap,
					fw);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private static void parsePathReviewsRates(String pathRevPath,
			Map<String, String> isbnRateMap, Writer writer) {
		LOGGER.log(Level.INFO, "Building <Path, #Reviews, RelScore> file..");
		int missingValues = 0;
		try {
			for (String line : Files.readAllLines(Paths.get(pathRevPath))) {
				String[] fields = line.split(",");
				String path = fields[0];
				String isbn = path.substring(path.lastIndexOf('/') + 1,
						path.lastIndexOf('.'));
				String score = isbnRateMap.get(isbn);
				if (score == null) {
					score = "0";
					missingValues++;
				}
				writer.write(line + "," + score + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO, "Missed value count: " + missingValues);
	}

	static Map<String, Integer> loadLtidTotalScoreMap(String qrelPath) {
		LOGGER.log(Level.INFO, "Loading Ltid -> Score map..");
		Map<String, Integer> ltidTotalScoreMap = new HashMap<String, Integer>();
		try {
			for (String line : Files.readAllLines(Paths.get(qrelPath))) {
				Pattern ptr = Pattern
						.compile("(\\d+)\\sQ?0\\s(\\w+)\\s([0-9])");
				Matcher m = ptr.matcher(line);
				if (m.find()) {
					if (!m.group(3).equals("0")) {
						String ltid = m.group(2);
						Integer score = Integer.parseInt(m.group(3));
						Integer sum = ltidTotalScoreMap.get(ltid);
						if (sum == null)
							sum = 0;
						ltidTotalScoreMap.put(ltid, sum + score);
					}
				} else {
					LOGGER.log(Level.WARNING, "regex failed for line: " + line);
				}
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO,
				"Ltid -> Score map size: " + ltidTotalScoreMap.size());
		return ltidTotalScoreMap;
	}

	static Map<String, String> loadIsbnRatingsMap(String pathRatePath) {
		Map<String, String> isbnRateMap = new HashMap<String, String>();
		try {
			for (String line : Files.readAllLines(Paths.get(pathRatePath))) {
				String[] fields = line.split(",");
				String path = fields[0];
				String isbn = path.substring(path.lastIndexOf('/') + 1,
						path.lastIndexOf('.'));
				String score = fields[2];
				isbnRateMap.put(isbn, score);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return isbnRateMap;
	}
}
