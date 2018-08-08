package irstyle;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;

import cache_selection_ml.FeatureExtraction;
import wiki13.WikiFileIndexer;

public class CacheSelectionQL {

	public static boolean useCache(String query, IndexReader cacheIndexReader, IndexReader globalIndexReader,
			IndexReader restIndexReader) throws IOException {
		System.out.println("  chacking cache selection..");
		FeatureExtraction fe = new FeatureExtraction(WikiFileIndexer.WEIGHT_ATTRIB);
		double ql_cache = 0;
		double ql_rest = 0;
		ql_cache = fe.queryLikelihood(cacheIndexReader, query, RelationalWikiIndexer.TEXT_FIELD, globalIndexReader,
				new StandardAnalyzer());
		ql_rest = fe.queryLikelihood(restIndexReader, query, RelationalWikiIndexer.TEXT_FIELD, globalIndexReader,
				new StandardAnalyzer());
		return (ql_cache >= ql_rest);
		// return false;
	}

}
