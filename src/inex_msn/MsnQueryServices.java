package inex_msn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

public class MsnQueryServices {

	public static void runUniQueries(List<MsnQueryDAO> queries, String resultFileName,
			String indexPath) {
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexPath)))) {
			System.out.println("Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			for (MsnQueryDAO queryDAO : queries) {
				// System.out.println(queryCoutner++);
				TopDocs topDocs = searcher.search(
						buildQuery(queryDAO.text, MsnExperiment.TITLE_ATTRIB,
								MsnExperiment.CONTENT_ATTRIB),
						MsnExperiment.MAX_HIT_COUNT);
				for (int i = 0; i < topDocs.scoreDocs.length; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					if (doc.get(MsnExperiment.DOCNAME_ATTRIB).equals(
							queryDAO.getFirstRelDoc())) {
						// //Prints the scoring logic for each query
						// System.out.println(searcher.explain(query,
						// hits[i].doc));
						queryDAO.mrr = 1.0 / (i + 1);
						break;
					}
				}
				int precisionBoundry = topDocs.scoreDocs.length > 10 ? 10
						: topDocs.scoreDocs.length;
				for (int i = 0; i < precisionBoundry; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					if (doc.get(MsnExperiment.DOCNAME_ATTRIB).equals(
							queryDAO.getFirstRelDoc())) {
						if (i < 3)
							queryDAO.p3 = 0.3;
						queryDAO.p10 = 0.1;
						break;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (FileWriter fw = new FileWriter(MsnExperiment.RESULT_DIR
				+ resultFileName)) {
			for (MsnQueryDAO query : queries) {
				fw.write(query.text + ", " + query.p3 + ", " + query.p10 + ", "
						+ query.mrr + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void runQueries(List<MsnQueryDAO> queries,
			String resultFileName, String indexPath) {
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
			for (MsnQueryDAO queryDAO : queries) {
				// System.out.println(queryCoutner++);
				TopDocs topDocs = searcher.search(
						buildQuery(queryDAO.text, MsnExperiment.TITLE_ATTRIB,
								MsnExperiment.CONTENT_ATTRIB), 10);
				int precisionBoundry = topDocs.scoreDocs.length > 10 ? 10
						: topDocs.scoreDocs.length;
				int sum = 0;
				for (int i = 0; i < precisionBoundry; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					String docName = doc.get(MsnExperiment.DOCNAME_ATTRIB);

					if (queryDAO.getRelDocs().contains(docName)) {
						sum++;
					}
				}
				queryDAO.p10 = sum / 10.0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (FileWriter fw = new FileWriter(MsnExperiment.RESULT_DIR
				+ resultFileName)) {
			for (MsnQueryDAO query : queries) {
				fw.write(query.text + ", " + query.p3 + ", " + query.p10 + ", "
						+ query.mrr + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Query buildQuery(String queryString, String field) {
		QueryParser parser = new QueryParser(field, new StandardAnalyzer());
		Query query = null;
		try {
			query = parser.parse(QueryParser.escape(queryString));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return query;
	}

	public static Query buildQuery(String queryString, String... fields) {
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

	@Deprecated
	protected Query buildBooleanQuery(String queryText) {
		Query query = null;
		QueryParser nameParser = new QueryParser(MsnExperiment.DOCNAME_ATTRIB,
				new StandardAnalyzer());
		Query nameQuery;
		Query contentQuery;
		try {
			nameQuery = nameParser.parse(QueryParser.escape(queryText));
			QueryParser contentParser = new QueryParser(
					MsnExperiment.CONTENT_ATTRIB, new StandardAnalyzer());
			contentQuery = contentParser.parse(QueryParser.escape(queryText));
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
			builder.add(nameQuery, BooleanClause.Occur.SHOULD);
			builder.add(contentQuery, BooleanClause.Occur.SHOULD);
			query = builder.build();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return query;
	}

	public static List<MsnQueryDAO> loadQueries(String queryFile) {
		List<MsnQueryDAO> queries = new ArrayList<MsnQueryDAO>();
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
					queries.add(new MsnQueryDAO(queryText, rel));
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

}
