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
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki_inex09.ClusterDirectoryInfo;
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
//		int expNo = Integer.parseInt(args[0]);
//		int totalCount = Integer.parseInt(args[1]);
		// float[] gammas = {0.25f, 0.25f, 0.25f, 0.25f};
		// expInex(expNo, totalCount, gammas);
//		buildGlobalIndex(expNo, totalCount);
		gridSearchExperiment();

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

	// Does the grid search to find best params. This code uses query time
	// boosting (vs. index time boosting)
	// to find the optimal param set.
	public static void gridSearchExperiment() {
		// Note that the path count should be sorted!
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle("/scratch/data-sets/imdb/mfullpath_votes.csv");
		LOGGER.log(Level.INFO,
				"Number of loaded path_counts: " + fileList.size());
		String indexName = "data/index/grid_imdb";
		LOGGER.log(Level.INFO, "Building index..");
		float[] fieldBoost = {1f, 1f, 1f, 1f, 1f};
		new ImdbIndexer().buildIndex(fileList, indexName, fieldBoost);
		LOGGER.log(Level.INFO, "Loading and running queries..");
		List<ExperimentQuery> queries = QueryServices.loadInexQueries(
				"data/queries/imdb/all-topics.csml",
				"data/queries/imdb/all.qresl");
		LOGGER.log(Level.INFO, "Submitting query.. #query = " + queries.size());
		List<List<QueryResult>> allResults = new ArrayList<List<QueryResult>>();
		for (int i = 0; i < 32; i++) {
			Map<String, Float> fieldToBoost = new HashMap<String, Float>();
			fieldToBoost.put(ImdbIndexer.TITLE_ATTRIB, (float) i % 2 + 1);
			fieldToBoost.put(ImdbIndexer.KEYWORDS_ATTRIB,
					((float) i / 2) % 2 + 1);
			fieldToBoost.put(ImdbIndexer.PLOT_ATTRIB, ((float) i / 4) % 2 + 1);
			fieldToBoost
					.put(ImdbIndexer.ACTORS_ATTRIB, ((float) i / 8) % 2 + 1);
			fieldToBoost.put(ImdbIndexer.REST_ATTRIB, ((float) i / 16) % 2 + 1);
			List<QueryResult> results = QueryServices.runQueriesWithBoosting(
					queries, indexName, new DefaultSimilarity(), fieldToBoost);
			allResults.add(results);
		}
		LOGGER.log(Level.INFO, "Writing results to file..");
		try (FileWriter fw = new FileWriter("data/result/grid_def.csv")) {
			for (int i = 0; i < queries.size(); i++) {
				fw.write(allResults.get(0).get(i).query.text + ",");
				for (int j = 0; j < allResults.size(); j++) {
					fw.write(allResults.get(j).get(i).precisionAtK(20) + ",");
				}
				fw.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		// try {
		// LOGGER.log(Level.INFO, "cleanup..");
		// FileUtils.deleteDirectory(new File(indexName));
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
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
				ImdbIndexer.KEYWORDS_ATTRIB, ImdbIndexer.PLOT_ATTRIB,
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

	public static void buildGlobalIndex(int expNo, int total) {
		List<InexFile> fileList = InexFile
				.loadFilePathCountTitle(ImdbClusterDirectoryInfo.FILE_LIST);
		LOGGER.log(Level.INFO, "Building index..");
		String indexName = ClusterDirectoryInfo.GLOBAL_INDEX_BASE + "imdb_"
				+ total + "_" + expNo;
		fileList = fileList.subList(0, (fileList.size() * expNo) / total);
		float[] fieldBoost = {1f, 1f, 1f, 1f, 1f};
		new ImdbIndexer().buildIndex(fileList, indexName, fieldBoost);
	}
}
