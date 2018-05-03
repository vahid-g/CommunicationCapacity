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
    public static Paths paths = new Paths("maple");

    public static void main(String[] args) throws IOException{
        paths = new Paths(args[0]);

        buildIndex(paths.allWiki13IndexPath);
//        buildIndex(paths.sub2Wiki13IndexPath);
//        buildIndex(paths.com2Wiki13IndexPath);
        getMSNQueryLikelihoods(paths.sub2Wiki13IndexPath, paths.allWiki13IndexPath,
                paths.defaultSavePath+"_sub2.txt", "jms");
        getMSNQueryLikelihoods(paths.sub2Wiki13IndexPath, paths.allWiki13IndexPath,
                paths.defaultSavePath+"_com2.txt", "jms");

    }

    public static void buildIndex(String indexPath) {
        long startTime = System.currentTimeMillis();
        List<InexFile> pathCountList = InexFile.loadInexFileList(paths.wiki13Count13Path);
        Analyzer analyzer = new StandardAnalyzer();
        WikiExperimentHelper.buildIndex(pathCountList, indexPath, analyzer, false);
        long endTime = System.currentTimeMillis();
        System.out.println((endTime-startTime)/1000);
    }

    public static void getMSNQueryLikelihoods(String indexPath, String globalIndexPath, String savePath,
                                              String difficultyMetric) throws IOException{
        List<ExperimentQuery> experimentQueries = QueryServices.loadMsnQueries(paths.msnQueryPath,
                paths.msnQrelPath);
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
