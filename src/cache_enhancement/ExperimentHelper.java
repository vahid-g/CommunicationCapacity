package cache_enhancement;

import indexing.InexFile;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiExperimentHelper;
import wiki13.querydifficulty.RunQueryDifficultyComputer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExperimentHelper {

    public static final Logger LOGGER = Logger.getLogger(ExperimentHelper.class.getName());
    public static PathCollection myPaths;

    public static void main(String[] args) throws IOException, ParseException {
        final String pathGroup = args[0];
        final String cacheUpdateLogPath = args[1];
        final String restUpdateLogPath = args[2];

        myPaths = PathCollection.get(pathGroup);
//        WikiIndexUpdater wikiIndexUpdater = new WikiIndexUpdater(myPaths.sub2Wiki13IndexPath,
//                myPaths.wiki13Count13Path);
//
//        wikiIndexUpdater.search("link", 10);

        System.out.println("Making copies of the Sub2 and Com2 Indexes ...");
        final String tempSub2Wiki13IndexPath = makeTempCopy(myPaths.sub2Wiki13IndexPath);
        final String tempCom2Wiki13IndexPath = makeTempCopy(myPaths.com2Wiki13IndexPath);

        System.out.println("Updating Sub2 index ...");
        updateIndex(tempSub2Wiki13IndexPath, cacheUpdateLogPath, myPaths.wiki13Count13Path);
        System.out.println("Updating Com2 index ...");
        updateIndex(tempCom2Wiki13IndexPath, restUpdateLogPath, myPaths.wiki13Count13Path);

        String sub2QueryLikelihoodPath = myPaths.defaultSavePath+"_sub2.txt";
        String com2QueryLikelihoodPath = myPaths.defaultSavePath+"_com2.txt";

        getMSNQueryLikelihoods(tempSub2Wiki13IndexPath, myPaths.allWiki13IndexPath,
                sub2QueryLikelihoodPath, "jms");
        getMSNQueryLikelihoods(tempCom2Wiki13IndexPath, myPaths.allWiki13IndexPath,
                com2QueryLikelihoodPath, "jms");


    }

    public static void buildWikiIndex(String indexPath, int expNo, boolean isComplement,
                                      String wikiCountPath, boolean isParallel) {
        long startTime = System.currentTimeMillis();
        Analyzer analyzer = new StandardAnalyzer();

        if (expNo >= 100) {
            List<InexFile> pathCountList = InexFile.loadInexFileList(wikiCountPath);
            WikiExperimentHelper.buildIndex(pathCountList, indexPath, analyzer, isParallel);
        }
        else if (isComplement) {
            WikiExperimentHelper.buildComplementIndex(expNo, 100, wikiCountPath,
                    indexPath, analyzer, isParallel);
        }
        else {
            WikiExperimentHelper.buildGlobalIndex(expNo, 100, wikiCountPath, indexPath,
                    analyzer, isParallel);
        }

        long endTime = System.currentTimeMillis();
        System.out.println((endTime-startTime)/1000);
    }

    public static void updateIndex(String indexPath, String updateLogFile, String wikiCountPath) throws IOException {
        WikiIndexUpdater wikiIndexUpdater = new WikiIndexUpdater(indexPath, wikiCountPath);
        List<UpdateDocument> updateDocuments = UpdateDocument.buildFromFile(updateLogFile);

        List<String> addLog = new ArrayList<String>();
        List<String> removeLog = new ArrayList<String>();
        for (UpdateDocument doc: updateDocuments) {
            if (doc.changeFlag == 'a') {
                addLog.add(doc.docNumber);
            }

            else if (doc.changeFlag == 'd') {
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

    public static String makeTempCopy(String indexPath) throws IOException {
        List<String> pathParts = new ArrayList<String>(Arrays.asList(indexPath.split("/", 0)));
        final String indexDir = pathParts.get(pathParts.size()-1);

        pathParts.remove(pathParts.size()-1);

        pathParts.add("temp");
        final String tempPath = String.join("/", pathParts);
        File tempDirectory = new File(tempPath);
        if(! tempDirectory.exists()) {
            tempDirectory.mkdirs();
        }

        pathParts.add(indexDir);
        final String newIndexPath = String.join("/", pathParts);

        final File src = new File(indexPath);
        final File dest = new File(newIndexPath);

        if(dest.exists()) {
            FileUtils.deleteDirectory(dest);
        }

        FileUtils.copyDirectory(src, dest, false);

        return dest.getPath();
    }

}
