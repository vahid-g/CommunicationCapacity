package wiki13.maple;

import java.util.List;

import indexing.InexFile;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperiment;

public class WikiMapleDocBoostExperiment {

    public static void main(String[] args) {
	List<InexFile> files = InexFile
		.loadInexFileList(WikiMapleExperiment.FILELIST_PATH);
	String indexPath = WikiMapleExperiment.DATA_PATH + "wiki_index/full";
	WikiExperiment.buildGlobalIndex(files, indexPath);
	List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
		WikiMapleExperiment.MSN_QUERY_FILE_PATH,
		WikiMapleExperiment.MSN_QREL_FILE_PATH);
	queries = QueryServices.loadInexQueries(
		WikiMapleExperiment.QUERY_FILE_PATH,
		WikiMapleExperiment.QREL_FILE_PATH, "title");
	List<QueryResult> results = WikiExperiment
		.runQueriesOnGlobalIndex(indexPath, queries, 0.15f, true);
	WikiExperiment.writeResultsToFile(results, "", "msn_100_docboost.csv");
    }
}
