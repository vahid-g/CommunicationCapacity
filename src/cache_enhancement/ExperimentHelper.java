package cache_enhancement;

import indexing.InexFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperimentHelper;
import wiki13.querydifficulty.RunQueryDifficultyComputer;

import java.io.*;
import java.util.*;
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

        System.out.println("Making copies of the Sub2 and Com2 Indexes ...");
        final String tempSub2Wiki13IndexPath = makeTempCopy(myPaths.sub2Wiki13IndexPath);
        final String tempCom2Wiki13IndexPath = makeTempCopy(myPaths.com2Wiki13IndexPath);

        System.out.println("Updating Sub2 index ...");
        updateIndex(tempSub2Wiki13IndexPath, cacheUpdateLogPath, myPaths.wiki13Count13Path);
        System.out.println("Updating Com2 index ...");
        updateIndex(tempCom2Wiki13IndexPath, restUpdateLogPath, myPaths.wiki13Count13Path);

        System.out.println("Calculating RRanks for Sub2  index ...");
        final String subRRankPath = getRRanks(tempSub2Wiki13IndexPath, "sub2_");

        String sub2QueryLikelihoodPath = myPaths.defaultSavePath+"query-assignments/"+
                myPaths.timestamp+"_llk_sub2.csv";
        String com2QueryLikelihoodPath = myPaths.defaultSavePath+"query-assignments/"+
                myPaths.timestamp+"_llk_com2.csv";

        System.out.println("Generating query likelihoods for Sub2 and Com2 ...");
        getMSNQueryLikelihoods(tempSub2Wiki13IndexPath, myPaths.allWiki13IndexPath,
                sub2QueryLikelihoodPath, "jms");
        getMSNQueryLikelihoods(tempCom2Wiki13IndexPath, myPaths.allWiki13IndexPath,
                com2QueryLikelihoodPath, "jms");

        Map<String, String> assignment = assignQueriesBasedonLikelihood(sub2QueryLikelihoodPath, "sub",
                com2QueryLikelihoodPath, "all");

        HashMap<String, String> fileLablePath = new HashMap<>();
        fileLablePath.put("sub", subRRankPath);
        fileLablePath.put("all", myPaths.allWikiRRanks);
        Double MRR = calculateMRR(assignment, fileLablePath);
        System.out.println("MRR: " + MRR);
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
        wikiIndexUpdater.commit();
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

    public static String getRRanks(String indexPath, String prefix) {
        List<ExperimentQuery> queries = QueryServices.loadMsnQueries(myPaths.msnQueryPath, myPaths.msnQrelPath);
        List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath,
                queries, 0.1f,false);
        final String saveDir = myPaths.defaultSavePath+ "rr/";
        final String filename = myPaths.timestamp + "_" + prefix +
                FilenameUtils.getName(myPaths.allWikiRRanks);
        WikiExperimentHelper.writeQueryResultsToFile(results, saveDir, filename);
        return saveDir+filename;
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

    public static Map<String, String> assignQueriesBasedonLikelihood(String likelihoodSubPath, String subLable,
                                                              String likelihoodComPath, String comLable) throws IOException {
        QueryLikelihood subLikelihoods = new QueryLikelihood(likelihoodSubPath, subLable);
        QueryLikelihood comLikelihoods = new QueryLikelihood(likelihoodComPath, comLable);
        Map<String, String> queryAssignment = QueryLikelihood.compareAndAssign(subLikelihoods, comLikelihoods);

        String dir = FilenameUtils.getFullPath(likelihoodComPath) + "query-assignments/";
        File directory = new File(dir);
        if (! directory.exists()) {
            directory.mkdir();
        }
        String saveAssignmentPath = dir + myPaths.timestamp + "_query_assignment.csv";
        try (FileWriter fw = new FileWriter(saveAssignmentPath)) {
            for (String query : queryAssignment.keySet()) {
                fw.write(query + ", " + queryAssignment.get(query) + "\n");
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        return  queryAssignment;
    }

    public static Double calculateMRR(Map<String, String> queryAssignment, Map<String,
            String> fileLablePath) throws IOException {
        final int queryFieldNumber = 0;
        final int mrrFieldNumber = 3;
        Double MRR = 0.0;
        int qCount = 0;

        for(String label: fileLablePath.keySet()) {
            File file = new File(fileLablePath.get(label));
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line;
            while((line = bufferedReader.readLine()) != null) {
                List<String> fields = CsvParsable.parse(line, ",");
                String query = fields.get(queryFieldNumber);
                Double rrank = Double.parseDouble(fields.get(mrrFieldNumber));
                if(queryAssignment.get(query).equals(label)) {
                    MRR += rrank;
                    qCount++;
                }
            }
        }

        if (queryAssignment.size() != qCount)
            LOGGER.log(Level.WARNING, "Not all queries are incorporated in calculating MRR.");

        MRR = MRR/qCount;
        return MRR;
    }

}
