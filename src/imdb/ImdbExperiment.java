package imdb;

import indexing.InexFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki_inex09.Utils;

public class ImdbExperiment {

	static final Logger LOGGER = Logger.getLogger(ImdbExperiment.class
			.getName());

	public static void main(String[] args) {

		long start_t = System.currentTimeMillis();
		// buildJmdbSortedPathRating("/scratch/data-sets/imdb/imdb-inex/movies");

		// float gamma1 = Float.parseFloat(args[0]);
		// float gamma2 = Float.parseFloat(args[1]);
		// gridSearchExperiment(gamma1, gamma2);
		int expNo = Integer.parseInt(args[0]);
		int totalCount = Integer.parseInt(args[1]);
		float[] gammas = {0.25f, 0.25f, 0.25f, 0.25f};
		expInex(expNo, totalCount, gammas);

		System.out.println((System.currentTimeMillis() - start_t) / 1000);
	}

	static List<InexFile> buildSortedPathRating(String datasetPath) {
		List<InexFile> pathCount = new ArrayList<InexFile>();
		List<String> filePaths = Utils
				.listFilesForFolder(new File(datasetPath));
		for (String filepath : filePaths) {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			try {
				DocumentBuilder db = dbf.newDocumentBuilder();
				org.w3c.dom.Document doc = db.parse(new File(filepath));
				NodeList nodeList = doc.getElementsByTagName("rating");
				if (nodeList.getLength() > 1) {
					LOGGER.log(Level.SEVERE, filepath
							+ " has more than one rating entries!");
				} else if (nodeList.getLength() < 1) {
					pathCount.add(new InexFile(filepath, 0.0));
				} else {
					Node node = nodeList.item(0).getFirstChild();
					if (node.getNodeValue() != null) {
						String rating = node.getNodeValue().split(" ")[0];
						pathCount.add(new InexFile(filepath, Double
								.parseDouble(rating)));
					} else {
						pathCount.add(new InexFile(filepath, 0.0));
					}
				}
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(pathCount);
		try (FileWriter fw = new FileWriter(ImdbClusterDirectoryInfo.FILE_LIST)) {
			for (InexFile dfm : pathCount) {
				fw.write(dfm.path + "," + dfm.weight + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (pathCount);
	}

	static List<InexFile> buildJmdbSortedPathRating(String datasetPath) {
		LOGGER.log(Level.INFO, "loading title ~> ratingss");
		Map<String, Integer> titleRating = new HashMap<String, Integer>();
		try (BufferedReader br = new BufferedReader(new FileReader(
				"data/movietitle_rating.csv"))) {
			String line = br.readLine();
			while (line != null) {
				String fields[] = line.split(";");
				if (fields.length == 3) {
					String title = fields[0];
					Integer numberOfVotes = Integer.parseInt(fields[1]);
					titleRating.put(title, numberOfVotes);
					if (title.contains("(VG)")) {
						title = title.replaceAll("(VG)", "");
						titleRating.put(title, numberOfVotes);
					} else if (title.contains("(V)")) {
						title = title.replaceAll("(V)", "");
						titleRating.put(title, numberOfVotes);
					} else if (title.contains("(TV)")) {
						title = title.replaceAll("(TV)", "");
						titleRating.put(title, numberOfVotes);
					}
				}
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, e.getStackTrace().toString());
			return null;
		} catch (IOException e1) {
			LOGGER.log(Level.SEVERE, e1.getStackTrace().toString());
		}

		LOGGER.log(Level.INFO, "loading path ~> rating");
		List<InexFile> pathCount = new ArrayList<InexFile>();
		List<String> filePaths = Utils
				.listFilesForFolder(new File(datasetPath));
		for (String filepath : filePaths) {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			try {
				DocumentBuilder db = dbf.newDocumentBuilder();
				org.w3c.dom.Document doc = db.parse(new File(filepath));
				NodeList nodeList = doc.getElementsByTagName("title");
				if (nodeList.getLength() > 1) {
					LOGGER.log(Level.SEVERE, filepath
							+ " has more than one title!");
				} else if (nodeList.getLength() < 1) {
					LOGGER.log(Level.SEVERE, filepath + " has no title!");
				} else {
					Node node = nodeList.item(0).getFirstChild();
					if (node.getNodeValue() != null) {
						String title = node.getNodeValue().trim();
						// find rating using title
						if (titleRating.containsKey(title)) {
							pathCount.add(new InexFile(filepath, titleRating
									.get(title)));
						} else {
							// LOGGER.log(Level.INFO,
							// "couldn't find ratings for title: " + title);
							pathCount.add(new InexFile(filepath, 0));
						}
					} else {
						LOGGER.log(Level.SEVERE, filepath + " has no title!");
					}
				}
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(pathCount);
		try (FileWriter fw = new FileWriter("data/imdb_path_ratings.csv")) {
			for (InexFile dfm : pathCount) {
				fw.write(dfm.path + "," + dfm.weight + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (pathCount);
	}

	// TODO: this code seems redundant. One can use expInex to do the grid
	// search
	public static void gridSearchExperiment(float gamma1, float gamma2) {
		// Note that the path count should be sorted!
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle(ImdbClusterDirectoryInfo.FILE_LIST);
		fileList = fileList.subList(0, fileList.size() / 10);
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + fileList.size());
		String indexName = ImdbClusterDirectoryInfo.LOCAL_INDEX + "grid"
				+ Float.toString(gamma1).replace(".", "")
				+ Float.toString(gamma2).replace(".", "");
		LOGGER.log(Level.INFO, "Building index..");
		new ImdbIndexer().buildIndex(fileList, indexName, gamma1, gamma2);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				ImdbClusterDirectoryInfo.QUERY_FILE,
				ImdbClusterDirectoryInfo.QREL_FILE);
		queries = queries.subList(0, queries.size());
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		String[] attribs = {ImdbIndexer.TITLE_ATTRIB, ImdbIndexer.ACTORS_ATTRIB};
		List<QueryResult> results = QueryServices.runQueries(queries,
				indexName, attribs);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(ImdbClusterDirectoryInfo.RESULT_DIR
				+ "grid_" + Float.toString(gamma1).replace(".", "")
				+ Float.toString(gamma2).replace(".", "") + ".csv");
				FileWriter fw2 = new FileWriter(
						ImdbClusterDirectoryInfo.RESULT_DIR + "grid_"
								+ Float.toString(gamma1).replace(".", "")
								+ Float.toString(gamma2).replace(".", "")
								+ ".top")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.toString() + "\n");
				fw2.write(mqr.top10() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void expInex(int expNo, int total, float... gamma) {
		// list should be sorted
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle(ImdbClusterDirectoryInfo.FILE_LIST);
		LOGGER.log(Level.INFO, "Building index..");
		String indexName = ImdbClusterDirectoryInfo.LOCAL_INDEX + "imdb_"
				+ expNo;
		fileList = fileList.subList(0, (fileList.size() * expNo) / total);
		HashMap<String, InexFile> idToInexFile = new HashMap<String, InexFile>();
		for (InexFile file : fileList) {
			idToInexFile.put(FilenameUtils.removeExtension(new File(file.path)
					.getName()), file);
		}
		new ImdbIndexer().buildIndex(fileList, indexName, gamma);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				ImdbClusterDirectoryInfo.QUERY_FILE,
				ImdbClusterDirectoryInfo.QREL_FILE);
		LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
		String queryAttribs[] = {ImdbIndexer.TITLE_ATTRIB,
				ImdbIndexer.GENRE_ATTRIB, ImdbIndexer.PLOT_ATTRIB,
				ImdbIndexer.ACTORS_ATTRIB};
		List<QueryResult> results = QueryServices.runQueries(queries,
				indexName, queryAttribs);
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter(ImdbClusterDirectoryInfo.RESULT_DIR
				+ "imdb_" + expNo + ".csv");
				FileWriter fw2 = new FileWriter(
						ImdbClusterDirectoryInfo.RESULT_DIR + "imdb_" + expNo
								+ ".log")) {
			for (QueryResult mqr : results) {
				fw.write(mqr.toString() + "\n");
				fw2.write(mqr.miniLog(idToInexFile) + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage());
		}
		try {
			LOGGER.log(Level.INFO, "cleanup..");
			FileUtils.deleteDirectory(new File(indexName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
