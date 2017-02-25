package inex09;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import old_experiments.Experiment13Old;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

public class InexQueryServices {

	final static int TOP_DOC_COUNT = 50;

	static final Logger LOGGER = Logger.getLogger(InexQueryServices.class
			.getName());

	public static List<InexQueryResult> runInexQueries(List<InexQuery> queries,
			String indexPath) {
		List<InexQueryResult> iqrList = new ArrayList<InexQueryResult>();
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexPath)))) {
			LOGGER.log(Level.INFO,"Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			for (InexQuery queryDAO : queries) {
				// LOGGER.log(Level.INFO,queryCoutner++);
				Query query = buildLuceneQuery(queryDAO.text,
						WikiIndexer.TITLE_ATTRIB, WikiIndexer.CONTENT_ATTRIB);
				TopDocs topDocs = searcher.search(query, TOP_DOC_COUNT);
				InexQueryResult iqr = new InexQueryResult(queryDAO);
				for (int i = 0; i < Math.min(TOP_DOC_COUNT,
						topDocs.scoreDocs.length); i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					String docName = doc.get(WikiIndexer.DOCNAME_ATTRIB);
					iqr.topResults.add(docName);
				}
				iqrList.add(iqr);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return iqrList;
	}
	
	public static List<MsnQueryResult> runMsnQueries(List<MsnQuery> queries,
			String indexPath) {
		List<MsnQueryResult> results = new ArrayList<MsnQueryResult>();
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexPath)))) {
			LOGGER.log(Level.INFO,"Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			// searcher.setSimilarity(new BM25Similarity());
			for (MsnQuery msnQuery : queries) {
				// LOGGER.log(Level.INFO,queryCoutner++);
				Query query = buildLuceneQuery(msnQuery.text,
						WikiIndexer.TITLE_ATTRIB, WikiIndexer.CONTENT_ATTRIB);
				TopDocs topDocs = searcher.search(query, TOP_DOC_COUNT);
				MsnQueryResult mqr = new MsnQueryResult(msnQuery);
				for (int i = 0; i < topDocs.scoreDocs.length; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					String docName = doc.get(WikiIndexer.DOCNAME_ATTRIB);
					if (i < 10) {
						String title = doc.get(WikiIndexer.TITLE_ATTRIB);
						mqr.results.add(docName + ": " + title);
					}
					if (msnQuery.qrels.contains(docName)) {
						mqr.rank = i + 1;
						break;
					}
					// TODO: handle multiple relevant answers
				}
				results.add(mqr);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return results;
	}

	public static Query buildLuceneQuery(String queryString, String... fields) {
		MultiFieldQueryParser parser = new MultiFieldQueryParser(fields,
				new StandardAnalyzer());
		parser.setDefaultOperator(Operator.OR);
		Query query = null;
		try {
			query = parser.parse(QueryParser.escape(queryString));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return query;
	}

	public static List<InexQuery> loadInexQueries(String queryFile) {
		List<InexQuery> queries = new ArrayList<InexQuery>();
		try (BufferedReader br = new BufferedReader(new FileReader(queryFile))) {
			String line;
			String queryText;
			String rel;
			String prevQueryText = "";
			while ((line = br.readLine()) != null) {
				String[] queryFields = line.split(",");
				queryText = queryFields[0].trim();
				rel = queryFields[2].trim();
				if (queryText.equals(prevQueryText)) {
					queries.get(queries.size() - 1).addRelevantAnswer(rel);
				} else {
					queries.add(new InexQuery(queryText, rel));
					prevQueryText = queryText;
				}
			}
			LOGGER.log(Level.INFO," # of loaded queries: " + queries.size());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return queries;
	}

	public static List<MsnQuery> loadMsnQueries(String queryFile,
			String qrelFile) {
		Map<String, List<String>> qidQrelMap = new HashMap<String, List<String>>();
		try (BufferedReader br = new BufferedReader(new FileReader(qrelFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				String qid = line.split(" ")[0];
				String qrel = line.split(" ")[2];
				if (qidQrelMap.containsKey(qid)) {
					List<String> rels = qidQrelMap.get(qid);
					rels.add(qrel);
				} else {
					List<String> rels = new ArrayList<String>();
					rels.add(qrel);
					qidQrelMap.put(qid, rels);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<MsnQuery> queryList = new ArrayList<MsnQuery>();
		try (BufferedReader br = new BufferedReader(new FileReader(queryFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				int index = line.lastIndexOf(" ");
				String text = line.substring(0, index).replace(",", "").replace("\"", "");
				String qid = line.substring(index + 1);
				if (qidQrelMap.containsKey(qid)) {
					List<String> qrels = qidQrelMap.get(qid);
					queryList.add(new MsnQuery(text, qrels, Integer
							.parseInt(qid)));
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return queryList;
	}

}
