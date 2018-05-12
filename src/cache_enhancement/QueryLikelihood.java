package cache_enhancement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryLikelihood extends CsvParsable {

    final public Map<String, Double> queryLikelihood;
    final String groupName;

    public QueryLikelihood(String csvFilePath, String name) throws IOException {
        groupName = name;
        queryLikelihood = buildFromFile(csvFilePath);
    }

    public static Map<String, Double> buildFromFile(String csvFilePath) throws IOException {
        Map<String, Double> queryLikelihoods = new HashMap<>();

        File csvFile = new File(csvFilePath);
        FileReader fileReader = new FileReader(csvFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        while((line = bufferedReader.readLine()) != null) {
            List<String> parts = parse(line);
            String query = parts.get(0);
            Double likelihood = Double.parseDouble(parts.get(1));
            queryLikelihoods.put(query, likelihood);
        }

        return queryLikelihoods;
    }

    public static Map<String, String> compareAndAssign(QueryLikelihood seri1, QueryLikelihood seri2) {
        Map<String, String> assignment = new HashMap<>();

        for(String q: seri1.queryLikelihood.keySet()) {
            if (seri1.queryLikelihood.get(q) <= seri2.queryLikelihood.get(q)){
                assignment.put(q, seri1.groupName);
            }
            else {
                assignment.put(q, seri2.groupName);
            }
        }
        return assignment;
    }

}
