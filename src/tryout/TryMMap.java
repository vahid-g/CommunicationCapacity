package tryout;

import java.util.List;

import query.ExperimentQuery;
import query.QueryServices;
import wiki13.WikiExperimentHelper;
import wiki13.WikiFilesPaths;

public class TryMMap {

	public static void main(String[] args) {
		WikiFilesPaths paths = WikiFilesPaths.getMaplePaths();
		String indexPath = paths.getIndexBase() + args[0];
		List<ExperimentQuery> queries = QueryServices.loadMsnQueries(paths.getMsnQueryFilePath(),
				paths.getMsnQrelFilePath());
		queries = queries.subList(0, 50);
		long startTime = System.currentTimeMillis();
		WikiExperimentHelper.runQueriesOnGlobalIndex(indexPath, queries, 0.1f);
		long endTime = System.currentTimeMillis();
		System.out.println((endTime - startTime));
	}

}
