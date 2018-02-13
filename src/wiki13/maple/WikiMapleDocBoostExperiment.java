package wiki13.maple;

import java.util.List;

import indexing.InexFile;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperimentHelper;

public class WikiMapleDocBoostExperiment {

    public static void main(String[] args) {
	List<InexFile> files = InexFile
		.loadInexFileList(WikiMaplePaths.FILELIST_PATH);
	String indexPath = WikiMaplePaths.DATA_PATH + "wiki_index/full";
	WikiExperimentHelper.buildGlobalIndex(files, indexPath);
	List<ExperimentQuery> queries = QueryServices.loadMsnQueries(
		WikiMaplePaths.MSN_QUERY_FILE_PATH,
		WikiMaplePaths.MSN_QREL_FILE_PATH);
	queries = QueryServices.loadInexQueries(
		WikiMaplePaths.QUERY_FILE_PATH,
		WikiMaplePaths.QREL_FILE_PATH, "title");
	List<QueryResult> results = WikiExperimentHelper
		.runQueriesOnGlobalIndex(indexPath, queries, 0.15f, true);
	WikiExperimentHelper.writeQueryResultsToFile(results, "", "msn_100_docboost.csv");
    }
}
