package query;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;

public class BoostedScoreQuery extends CustomScoreQuery {

    private String boostFieldLabel;

    public BoostedScoreQuery(Query subQuery, String boostFieldLabel) {
	super(subQuery);
	this.boostFieldLabel = boostFieldLabel;
    }

    private class BoostedScoreProvider extends CustomScoreProvider {

	private LeafReader reader;
	private Set<String> fieldsToLoad = new HashSet<String>();

	public BoostedScoreProvider(LeafReaderContext context,
		String boostFieldLabel) {
	    super(context);
	    reader = context.reader();
	    fieldsToLoad.add(boostFieldLabel);
	}

	@Override
	public float customScore(int doc_id, float score, float valSrcScore)
		throws IOException {
	    Document doc = reader.document(doc_id, fieldsToLoad);
	    IndexableField field = doc.getField(boostFieldLabel);
	    double weight = field.numericValue().doubleValue();
	    if (weight == 0)
		weight = 1;
	    return (float) (Math.log(score) + Math.log(weight));
	}
    }

    @Override
    public CustomScoreProvider getCustomScoreProvider(
	    LeafReaderContext context) {
	return new BoostedScoreProvider(context, boostFieldLabel);
    }
}
