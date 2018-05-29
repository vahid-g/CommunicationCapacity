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

public class EnhanceHelper {

    public static final Logger LOGGER = Logger.getLogger(EnhanceHelper.class.getName());
    public static PathCollection myPaths;

    public static void main(String[] args) throws IOException {
        final String pathGroupName = args[0];
        myPaths = PathCollection.get(pathGroupName);

//        final String cacheUpdateLogPath = args[1];
//        final String restUpdateLogPath = args[2];
//        enhance(pathGroup, cacheUpdateLogPath, restUpdateLogPath);

        final String assignmentPath = args[1];
        final String updateAssignmentPath = args[2];
        enhance2(updateAssignmentPath, assignmentPath);
    }

    private static Double enhance(String cacheUpdateLogPath, String restUpdateLogPath) throws IOException {
        /**
         * Enhance by adding and removing tuples from the cache
         */
        final String tempSub2Wiki13IndexPath = makeTempCopy(myPaths.sub2Wiki13IndexPath);
        final String tempCom2Wiki13IndexPath = makeTempCopy(myPaths.com2Wiki13IndexPath);

        updateIndex(tempSub2Wiki13IndexPath, cacheUpdateLogPath, myPaths.wiki13Count13Path, myPaths.inex_13_text);
        updateIndex(tempCom2Wiki13IndexPath, restUpdateLogPath, myPaths.wiki13Count13Path, myPaths.inex_13_text);

        final String subRRankPath = getRRanks(tempSub2Wiki13IndexPath, "sub2_");

        String sub2QueryLikelihoodPath = myPaths.defaultSavePath+"query-assignments/"+
                myPaths.timestamp+"_llk_sub2.csv";
        String com2QueryLikelihoodPath = myPaths.defaultSavePath+"query-assignments/"+
                myPaths.timestamp+"_llk_com2.csv";

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
        return MRR;
    }

    private static Double enhance2(String updatedAssignmentFilePath, String assignmentFilePath) throws IOException {
        /**
         * Enhance by modifying the query assignments (to sub or all) based on the difficulty level of the queries
         */
        List<List<String>> update = CsvParsable.parseFile(updatedAssignmentFilePath, ",", 2, true);
        List<List<String>> previous = CsvParsable.parseFile(assignmentFilePath, ",", 2, true);

        Map<String, String> updateAssignment = new HashMap<>();
        for (List<String> line: update) {
            updateAssignment.put(line.get(0), line.get(1));
        }
        Map<String, String> queryAssignment = new HashMap<>();
        int updateCount = 0;
        for (List<String> line: previous) {
            final String query = line.get(0);
            final String prevGroup = line.get(1);
            final String group = updateAssignment.getOrDefault(query, prevGroup);
            if (! group.equals(prevGroup)) updateCount++;
            queryAssignment.put(query, group);
        }

        HashMap<String, String> fileLablePath = new HashMap<>();
        fileLablePath.put("sub", "/data/khodadaa/lucene-index/rr/05092047_sub2_rranks.csv");
        fileLablePath.put("all", myPaths.allWikiRRanks);
        Double MRR = calculateMRR(queryAssignment, fileLablePath);
        LOGGER.info("Number of queries with changed assignment: " + updateCount);
        LOGGER.info("MRR: " + MRR);
        return MRR;
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
        LOGGER.log(Level.INFO, String.format("[ExpNo: %d%%]-[Complement: %b] index is built in [%s] in [%.2f]sec",
                expNo, isComplement, indexPath, (endTime-startTime)/1000.0));
    }

    public static void updateIndex(String indexPath, String updateLogFile, String wikiCountPath,
                                   String inexDocsRootDir) throws IOException {
        LOGGER.log(Level.INFO, String.format("Updating [%s] based on log file [%s] and WikiCount [%s]",
                indexPath, updateLogFile, wikiCountPath));
        WikiIndexUpdater wikiIndexUpdater = new WikiIndexUpdater(indexPath, wikiCountPath, inexDocsRootDir);
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
        int addFailure = wikiIndexUpdater.addDoc(addLog);
        int removeFailure = wikiIndexUpdater.removeDoc(removeLog);
        wikiIndexUpdater.commit();
        LOGGER.log(Level.INFO, String.format("Add-failure %d(%d), remove-failure %d(%d), for index [%s]",
                addFailure, addLog.size(), removeFailure, removeLog.size(), indexPath));
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

        LOGGER.log(Level.INFO, String.format("Saving [%s] query scores for [%s] in [%s]",
                difficultyMetric, indexPath, savePath));
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
        LOGGER.log(Level.INFO, String.format("Saving MSN Queries' RRanks on index [] in []",
                indexPath, saveDir+filename));
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

        LOGGER.log(Level.INFO, String.format("Copying [%s]  ->  [%s]", src, dest));
        FileUtils.copyDirectory(src, dest, false);

        return dest.getPath();
    }

    public static Map<String, String> assignQueriesBasedonLikelihood(String likelihoodSubPath, String subLable,
                                                              String likelihoodComPath, String comLable) throws IOException {
        QueryLikelihood subLikelihoods = new QueryLikelihood(likelihoodSubPath, subLable);
        QueryLikelihood comLikelihoods = new QueryLikelihood(likelihoodComPath, comLable);
        Map<String, String> queryAssignment = QueryLikelihood.compareAndAssign(subLikelihoods, comLikelihoods);

        String dir = FilenameUtils.getFullPath(likelihoodComPath);
        File directory = new File(dir);
        if (! directory.exists()) {
            directory.mkdir();
        }

        String saveAssignmentPath = dir + myPaths.timestamp + "_query_assignment.csv";
        LOGGER.log(Level.INFO, String.format("Saving query assignment in [%s]", saveAssignmentPath));
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
