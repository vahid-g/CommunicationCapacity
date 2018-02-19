package query;

import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.Query;

public class LuceneQueryBuilder {

	private Map<String, Float> fieldBoostMap;

	private String docBoostFieldName;

	public LuceneQueryBuilder(Map<String, Float> fieldBoostMap) {
		this.fieldBoostMap = fieldBoostMap;
	}

	public LuceneQueryBuilder(Map<String, Float> fieldBoostMap, String docBoostFieldName) {
		this(fieldBoostMap);
		this.docBoostFieldName = docBoostFieldName;
	}

	public Query buildQuery(String queryText) {
		MultiFieldQueryParser multiFieldParser = new MultiFieldQueryParser(
				fieldBoostMap.keySet().toArray(new String[0]), new StandardAnalyzer(), fieldBoostMap);
		multiFieldParser.setDefaultOperator(Operator.OR);
		Query query = null;
		try {
			query = multiFieldParser.parse(QueryParser.escape(queryText));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		if (docBoostFieldName != null) {
			return new BoostedScoreQuery(query, docBoostFieldName);
		} else {
			return query;
		}
	}

}
