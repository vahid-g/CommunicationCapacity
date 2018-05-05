package cache_enhancement;

import indexing.InexFile;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiExperimentHelper;
import wiki13.querydifficulty.RunQueryDifficultyComputer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExperimentHelper {

    public static final Logger LOGGER = Logger.getLogger(ExperimentHelper.class.getName());
    public static PathCollection myPaths;

    public static void main(String[] args) throws IOException{
        final String pathGroup = args[0];
        final String cacheUpdateLogPath = args[1];
        final String restUpdateLogPath = args[2];

        myPaths = PathCollection.get(pathGroup);

        duplicateIndex(myPaths.sub2Wiki13IndexPath);
        updateIndex(myPaths.sub2Wiki13IndexPath, cacheUpdateLogPath);
        duplicateIndex(myPaths.com2Wiki13IndexPath);
        updateIndex(myPaths.com2Wiki13IndexPath, restUpdateLogPath);

        String sub2QueryLikelihoodPath = myPaths.defaultSavePath+"_sub2.txt";
        String com2QueryLikelihoodPath = myPaths.defaultSavePath+"_com2.txt";

        getMSNQueryLikelihoods(myPaths.sub2Wiki13IndexPath, myPaths.allWiki13IndexPath,
                sub2QueryLikelihoodPath, "jms");
        getMSNQueryLikelihoods(myPaths.com2Wiki13IndexPath, myPaths.allWiki13IndexPath,
                com2QueryLikelihoodPath, "jms");



    }

    public static void buildIndex(String indexPath) {
        long startTime = System.currentTimeMillis();
        List<InexFile> pathCountList = InexFile.loadInexFileList(myPaths.wiki13Count13Path);
        Analyzer analyzer = new StandardAnalyzer();
        WikiExperimentHelper.buildIndex(pathCountList, indexPath, analyzer, false);
        long endTime = System.currentTimeMillis();
        System.out.println((endTime-startTime)/1000);
    }

    public static void updateIndex(String indexPath, String updateLogFile) {
        WikiIndexUpdater wikiIndexUpdater = new WikiIndexUpdater(indexPath, myPaths.wiki13Count13Path);
        List<UpdateDocument> updateDocuments = UpdateDocument.build(updateLogFile);
        List<String> addLog = new ArrayList<String>();
        List<String> removeLog = new ArrayList<String>();
        for (UpdateDocument doc: updateDocuments) {
            if (doc.flag == 'a') {
                addLog.add(doc.docNumber);
            }

            else if (doc.flag == 'r') {
                removeLog.add(doc.docNumber);
            }
        }
        wikiIndexUpdater.addDoc(addLog);
        wikiIndexUpdater.removeDoc(removeLog);
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

    public static void duplicateIndex(String indexPath) {
        throw new NotImplementedException("");
    }

}
