package wiki13;

import indexing.InexDatasetIndexer;
import indexing.InexFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;

public class WikiExperiment {

    public static final Logger LOGGER = Logger.getLogger(WikiExperiment.class
	    .getName());

    public static void main(String[] args) {

    }

    public static void buildGlobalIndex(int expNo, int totalExp,
	    String filelistPopularityPath, String indexPath) {
	try {
	    List<InexFile> pathCountList = InexFile
		    .loadInexFileList(filelistPopularityPath);
	    double total = (double) totalExp;
	    pathCountList = pathCountList.subList(0,
		    (int) (((double) expNo / total) * pathCountList.size()));
	    LOGGER.log(Level.INFO, "Number of loaded path_counts: "
		    + pathCountList.size());
	    LOGGER.log(Level.INFO, "Best score: " + pathCountList.get(0).weight);
	    LOGGER.log(
		    Level.INFO,
		    "Smallest score: "
			    + pathCountList.get(pathCountList.size() - 1).weight);
	    buildGlobalIndex(pathCountList, indexPath);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public static void buildGlobalIndex(List<InexFile> files, String indexPath) {
	try {
	    File indexPathFile = new File(indexPath);
	    if (!indexPathFile.exists()) {
		indexPathFile.mkdirs();
	    }
	    LOGGER.log(Level.INFO, "Building index at: " + indexPath);
	    WikiFileIndexer fileIndexer = new WikiFileIndexer();
	    InexDatasetIndexer idi = new InexDatasetIndexer(fileIndexer);
	    idi.buildIndex(files, indexPath);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public static List<QueryResult> runQueriesOnGlobalIndex(String indexPath,
	    List<ExperimentQuery> queries, float gamma) {
	LOGGER.log(Level.INFO, "Number of loaded queries: " + queries.size());
	Map<String, Float> fieldToBoost = new HashMap<String, Float>();
	fieldToBoost.put(WikiFileIndexer.TITLE_ATTRIB, gamma);
	fieldToBoost.put(WikiFileIndexer.CONTENT_ATTRIB, 1 - gamma);
	List<QueryResult> results = QueryServices.runQueriesWithBoosting(
		queries, indexPath, new BM25Similarity(), fieldToBoost, false);
	return results;
    }

    public static void writeResultsToFile(List<QueryResult> results,
	    String resultDirPath, String resultFileName) {
	LOGGER.log(Level.INFO, "Writing results..");
	File resultDir = new File(resultDirPath);
	if (!resultDir.exists()) {
	    resultDir.mkdirs();
	}
	try (FileWriter fw = new FileWriter(resultFileName)) {
	    for (QueryResult iqr : results) {
		fw.write(iqr.resultString() + "\n");
	    }
	} catch (IOException e) {
	    LOGGER.log(Level.SEVERE, e.getMessage(), e);
	}
    }

    public static List<Double> computeQueryDifficulty(String indexPath,
    	    List<ExperimentQuery> queries, String field) {
    	List<Double> difficulties = new ArrayList<Double>();
    	try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
    		.get(indexPath)))) {
    	    computeQueryDifficulty(reader, queries, field);
    	} catch (IOException e) {
    	    LOGGER.log(Level.SEVERE, e.getMessage(), e);
    	}
    	return difficulties;
        }

    public static List<Double> computeQueryDifficulty(IndexReader reader,
    	    List<ExperimentQuery> queries, String field) throws IOException {
    	List<Double> difficulties = new ArrayList<Double>();
    	long titleTermCount = reader.getSumTotalTermFreq(field);
	LOGGER.log(Level.INFO, "Total number of terms in " + field + ": "
		+ titleTermCount);
    	for (ExperimentQuery query : queries) {
    	    List<String> terms = Arrays
    		    .asList(query.getText().split("[ \"'+]")).stream()
    		    .filter(str -> !str.isEmpty()).collect(Collectors.toList());
    	    int qLength = terms.size();
    	    long termCountSum = 0;
    	    for (String term : terms) {
    		termCountSum += reader.totalTermFreq(new Term(field, term));
    	    }
    	    double ictf = Math.log(titleTermCount / (termCountSum + 1.0));
    	    difficulties.add(1.0 / qLength + ictf / qLength);
    	}
    	return difficulties;
        }

}
