package cache_enhancement;

import indexing.InexFile;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiExperimentHelper;
import wiki13.querydifficulty.RunQueryDifficultyComputer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExperimentHelper {

    public static final Logger LOGGER = Logger.getLogger(ExperimentHelper.class.getName());
    public static PathCollection myPaths = PathCollection.get("maple");

    public static void main(String[] args) throws IOException{
        myPaths = PathCollection.get("maple");
        System.out.println(myPaths.allWiki13IndexPath);

        buildIndex(myPaths.allWiki13IndexPath);
//        buildIndex(myPaths.sub2Wiki13IndexPath);
//        buildIndex(myPaths.com2Wiki13IndexPath);
        getMSNQueryLikelihoods(myPaths.sub2Wiki13IndexPath, myPaths.allWiki13IndexPath,
                myPaths.defaultSavePath+"_sub2.txt", "jms");
        getMSNQueryLikelihoods(myPaths.sub2Wiki13IndexPath, myPaths.allWiki13IndexPath,
                myPaths.defaultSavePath+"_com2.txt", "jms");

    }

    public static void buildIndex(String indexPath) {
        long startTime = System.currentTimeMillis();
        List<InexFile> pathCountList = InexFile.loadInexFileList(myPaths.wiki13Count13Path);
        Analyzer analyzer = new StandardAnalyzer();
        WikiExperimentHelper.buildIndex(pathCountList, indexPath, analyzer, false);
        long endTime = System.currentTimeMillis();
        System.out.println((endTime-startTime)/1000);
    }

    public static void updateIndex(String indexPath, String file) {

    }

    public static void getMSNQueryLikelihoods(String indexPath, String globalIndexPath, String savePath,
                                              String difficultyMetric) throws IOException{
        List<ExperimentQuery> experimentQueries = QueryServices.loadMsnQueries(myPaths.msnQueryPath,
                myPaths.msnQrelPath);
        RunQueryDifficultyComputer qdc = new RunQueryDifficultyComputer();
        List<String> scores;
        try {
            scores = qdc.runQueryScoreComputer(indexPath, globalIndexPath,
                    experimentQueries, difficultyMetric);
        } catch (org.apache.commons.cli.ParseException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            return;
        }

        try (FileWriter fw = new FileWriter(savePath)) {
            for (int i = 0; i < experimentQueries.size(); i++) {
                fw.write(experimentQueries.get(i).getText() + ", " + scores.get(i) + "\n");
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
