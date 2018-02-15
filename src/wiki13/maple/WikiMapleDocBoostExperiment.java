package wiki13.maple;

import java.util.List;

import indexing.InexFile;
import query.ExperimentQuery;
import query.QueryResult;
import query.QueryServices;
import wiki13.WikiExperimentHelper;
import wiki13.WikiFilesPaths;

public class WikiMapleDocBoostExperiment {

	private static WikiFilesPaths PATHS = WikiFilesPaths.getMaplePaths();

	public static void main(String[] args) {
		List<InexFile> files = InexFile.loadInexFileList(PATHS.getAccessCountsPath());
		String indexPath = PATHS.getIndexBase() + "100";
		WikiExperimentHelper.buildGlobalIndex(files, indexPath);
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(PATHS.getMsnQueryFilePath(),
				PATHS.getMsnQrelFilePath());
		queries = QueryServices.loadInexQueries(PATHS.getInexQueryFilePath(), PATHS.getInexQrelFilePath(), "title");
		List<QueryResult> results = WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.15f, true);
		WikiExperimentHelper.writeQueryResultsToFile(results, "", "msn_100_docboost.csv");
	}
}
