package cache_enhancement;

import indexing.InexFile;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperimentHelper;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class QueryExecuter {
//    public static final String queryPath = "/data/khodadaa/lucene-index/msn_query_qid.csv";
    public static final String queryPath = "data/msn_query_qid.csv";
//    public static final String qrelPath = "/data/khodadaa/lucene-index/msn.qrels";
    public static final String qrelPath = "data/msn.qrels";
//    public static final String accessCountsFilePath = "/data/wikipedia/wiki13_counts13_title.csv";
    public static final String accessCountsFilePath = "data/wiki13_counts13_title.csv";
    public static final String indexPath = "data/wiki13_index";

    public static void main(String[] args) {

//        Scanner input = new Scanner(System.in);
//        System.out.println(args[0]);
//        input.nextLine();

        List<ExperimentQuery> experimentQueries = QueryServices.loadMsnQueries(queryPath, qrelPath);
        List<QueryResult> queryResults = WikiExperimentHelper.runQueriesOnGlobalIndex(args[0],
                experimentQueries, 0.1f);
        System.out.println(Arrays.toString(queryResults.toArray()));

    }

    public static void buildIndex() {
        long startTime = System.currentTimeMillis();
        List<InexFile> pathCountList = InexFile.loadInexFileList(accessCountsFilePath);
        Analyzer analyzer = new StandardAnalyzer();
        WikiExperimentHelper.buildIndex(pathCountList, indexPath, analyzer, false);
        long endTime = System.currentTimeMillis();
        System.out.println((endTime-startTime)/1000);
    }
}
