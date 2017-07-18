package amazon;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import indexing.InexFile;
import wiki_inex09.Utils;

public class AmazonUtils {

	static final Logger LOGGER = Logger.getLogger(AmazonUtils.class.getName());

	static int missingValues = 0;

	public static void main(String[] args) {
		// buildPathPopularityWithRatings();
		// buildPathRelScoreList();
		buildPathReviewRateList();
	}

	public static List<InexFile> buildSortedPathReviewsList(String datasetPath) {
		List<InexFile> pathCount = new ArrayList<InexFile>();
		List<String> filePaths = Utils.listFilesForFolder(new File(datasetPath));
		for (String filepath : filePaths) {
			if (filepath.contains(".dtd"))
				continue;
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			try {
				DocumentBuilder db = dbf.newDocumentBuilder();
				org.w3c.dom.Document doc = db.parse(new File(filepath));
				NodeList nodeList = doc.getElementsByTagName("review");
				pathCount.add(new InexFile(filepath, nodeList.getLength()));
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(pathCount);
		try (FileWriter fw = new FileWriter(AmazonDirectoryInfo.FILE_LIST)) {
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
			parseRatings(fr, fw);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	// Processes the UCSD book rating information and builds a path -> rating
	// file
	static void parseRatings(Reader reader, Writer writer) {
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
							writer.write(folder + "/" + lastIsbn + ".xml," + totalRate + "," + count + "\n");
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

	public static void buildPathRelScoreList() {
		try (FileWriter fw = new FileWriter("data/amazon_path_reviews_rels.csv")) {
			Map<String, Set<Integer>> ltidScoresMap = loadLtidRelScoreMap("data/inex14sbs.qrels");
			Map<String, String> isbnLtidMap = AmazonUtils.loadIsbnLtidMap("data/amazon-lt.isbn.thingID.csv");
			parsePathReviewsRels("data/amazon_path_reviews.csv", ltidScoresMap, isbnLtidMap, fw);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	static void parsePathReviewsRels(String pathRevPath, Map<String, Set<Integer>> ltidScoresMap,
			Map<String, String> isbnLtidMap, Writer writer) {
		LOGGER.log(Level.INFO, "Building <Path, #Reviews, RelScore> file..");
		try {
			for (String line : Files.readAllLines(Paths.get(pathRevPath))) {
				String[] fields = line.split(",");
				String path = fields[0];
				String isbn = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.'));
				String ltid = isbnLtidMap.get(isbn);
				Integer score = 0;
				if (ltidScoresMap.containsKey(ltid)) {
					for (Integer i : ltidScoresMap.get(ltid)) {
						score += i;
					}
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

	static Map<String, Set<Integer>> loadLtidRelScoreMap(String qrelPath) {
		LOGGER.log(Level.INFO, "Loading Ltid -> Score map..");
		HashMap<String, Set<Integer>> ltidRelScores = new HashMap<String, Set<Integer>>();
		try {
			for (String line : Files.readAllLines(Paths.get(qrelPath))) {
				Pattern ptr = Pattern.compile("(\\d+)\\sQ?0\\s(\\w+)\\s([0-9])");
				Matcher m = ptr.matcher(line);
				if (m.find()) {
					if (!m.group(3).equals("0")) {
						String ltid = m.group(2);
						Integer score = Integer.parseInt(m.group(3));
						Set<Integer> scores = ltidRelScores.get(ltid);
						if (scores == null) {
							scores = new HashSet<Integer>();
							scores.add(score);
							ltidRelScores.put(ltid, scores);
						} else {
							scores.add(score);
						}
					}
				} else {
					LOGGER.log(Level.WARNING, "regex failed for line: " + line);
				}
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		LOGGER.log(Level.INFO, "Ltid -> Score map size: " + ltidRelScores.size());
		return ltidRelScores;
	}

	public static void parsePopularity(Reader reader, Writer writer) {
		try (BufferedReader br = new BufferedReader(reader)) {
			String line = br.readLine();
			String lastIsbn = "0000000000";
			Pattern ptr = Pattern.compile(".+,([0-9X]{10}),([0-9.]+),\\d+");
			int rate = 0;
			while (line != null) {
				Matcher matcher = ptr.matcher(line);
				if (matcher.find()) {
					String isbn = matcher.group(1);
					if (lastIsbn.equals(isbn)) {
						rate++;
					} else {
						String folder = lastIsbn.substring(lastIsbn.length() - 3);
						if (rate != 0) { // this if is to skip the first line
							writer.write(folder + "/" + lastIsbn + ".xml," + rate + "\n");
						}
						rate = 1;
					}
					lastIsbn = isbn;
				} else {
					LOGGER.log(Level.SEVERE, "Couldn't parse line: " + line);
					break;
				}
				line = br.readLine();
			}
			String folder = lastIsbn.substring(lastIsbn.length() - 3);
			writer.write(folder + "/" + lastIsbn + ".xml," + rate);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	static Map<String, String> loadIsbnLtidMap(String path) {
		LOGGER.log(Level.INFO, "Loading Isbn -> Ltid map..");
		Map<String, String> isbnToLtid = new HashMap<String, String>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			String line = br.readLine();
			while (line != null) {
				String[] ids = line.split(",");
				isbnToLtid.put(ids[0], ids[1]);
				line = br.readLine();
			}
			LOGGER.log(Level.INFO, "Isbn -> Ltid map size: " + isbnToLtid.size());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return isbnToLtid;
	}

	public static void buildPathReviewRateList() {
		try (FileWriter fw = new FileWriter("data/amazon_path_reviews_rate.csv")) {
			Map<String, String> isbnRateMap = loadIsbnRatingsMap("data/amazon_path_rate.csv");
			LOGGER.log(Level.SEVERE, "Size of IsbnRatings map: " + isbnRateMap.size());
			parsePathReviewsRates("data/amazon_path_reviews.csv", isbnRateMap, fw);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private static Map<String, String> loadIsbnRatingsMap(String pathRatePath) {
		Map<String, String> isbnRateMap = new HashMap<String, String>();
		try {
			for (String line : Files.readAllLines(Paths.get(pathRatePath))) {
				String[] fields = line.split(",");
				String path = fields[0];
				String isbn = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.'));
				String score = fields[2];
				isbnRateMap.put(isbn, score);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
		return isbnRateMap;
	}

	private static void parsePathReviewsRates(String pathRevPath, Map<String, String> isbnRateMap, Writer writer) {
		LOGGER.log(Level.INFO, "Building <Path, #Reviews, RelScore> file..");
		int missingValues = 0;
		try {
			for (String line : Files.readAllLines(Paths.get(pathRevPath))) {
				String[] fields = line.split(",");
				String path = fields[0];
				String isbn = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.'));
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
}
