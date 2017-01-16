package inex09;

import inex13.Experiment;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.json.JSONObject;

public class InexQueryServices {

	public static void runInexQueries(List<InexQueryDAO> queries,
			String resultFile, String indexPath) {
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexPath)))) {
			// // Code to print the terms of index
			// Fields fields = MultiFields.getFields(reader);
			// for (String field : fields) {
			// Terms terms = fields.terms(field);
			// TermsEnum termsEnum = terms.iterator();
			// while (termsEnum.next() != null) {
			// System.out.println(termsEnum.term().utf8ToString());
			// }
			// }
			System.out.println("Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			for (InexQueryDAO queryDAO : queries) {
				// System.out.println(queryCoutner++);
				Query query = buildLuceneQuery(queryDAO.text,
						Experiment.TITLE_ATTRIB, Experiment.CONTENT_ATTRIB);
				TopDocs topDocs = searcher.search(query, 10);
				int precisionBoundry = topDocs.scoreDocs.length > 10
						? 10
						: topDocs.scoreDocs.length;
				int sum = 0;
				for (int i = 0; i < precisionBoundry; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					String docName = doc.get(Experiment.DOCNAME_ATTRIB);

					if (queryDAO.getRelDocs().contains(docName)) {
						sum++;
					}
				}
				queryDAO.p10 = sum / 10.0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (FileWriter fw = new FileWriter(resultFile)) {
			for (InexQueryDAO query : queries) {
				fw.write(query.text + ", " + query.p3 + ", " + query.p10 + ", "
						+ query.mrr + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List<MsnQueryResult> runMsnQueries(List<MsnQuery> queries, String indexPath) {
		List<MsnQueryResult> results = new ArrayList<MsnQueryResult>();
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexPath)))) {
			System.out.println("Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			for (MsnQuery msnQuery : queries) {
				// System.out.println(queryCoutner++);
				Query query = buildLuceneQuery(msnQuery.text,
						Experiment.TITLE_ATTRIB, Experiment.CONTENT_ATTRIB);
				TopDocs topDocs = searcher.search(query, 10);
				MsnQueryResult mqr = new MsnQueryResult(msnQuery);
				for (int i = 0; i < topDocs.scoreDocs.length; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					String docName = doc.get(Experiment.DOCNAME_ATTRIB);
					if (msnQuery.qrels.contains(docName)) {
						mqr.rank = i + 0;
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

	public static List<InexQueryDAO> loadInexQueries(String queryFile) {
		List<InexQueryDAO> queries = new ArrayList<InexQueryDAO>();
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
					queries.add(new InexQueryDAO(queryText, rel));
					prevQueryText = queryText;
				}
			}
			System.out.println(" # of loaded queries: " + queries.size());
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
				String qrel = line.split(" ")[3];
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
				String text = line.substring(0, index);
				String qid = line.substring(index + 1);
				if (qidQrelMap.containsKey(qid)) {
					List<String> qrels = qidQrelMap.get(qid);
					queryList.add(new MsnQuery(text, qrels));
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
