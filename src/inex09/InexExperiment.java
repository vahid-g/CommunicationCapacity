package inex09;

import inex13.Experiment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class InexExperiment {

	public static final int MAX_HIT_COUNT = 100;

	public static final String INDEX_DIR = "data/index/";
	public static final String RESULT_DIR = "data/result/";
	public static final String QUERY_DIR = "data/queries/inex09/";
	public static final String DEFAULT_QUERY_FILE = QUERY_DIR
			+ "queries.csv";
	public static final String DATASET_DIR = "/scratch/data-sets/inex_09/000";

	static int queryCoutner = 0;

	private String indexDirPath;

	public InexExperiment(String indexName) {
		indexDirPath = INDEX_DIR + indexName + "/";
	}

	public static void main(String[] args) {
		exp0();
	}

	public static void exp0() { // indexing and querying whole dataset
		InexExperiment ie = new InexExperiment("inex9_xml");
		Date start = new Date();
		InexIndexer.buildIndex(DATASET_DIR, ie.indexDirPath);
		Date endIndexing = new Date();
		System.out.println("Indexing time: "
				+ (endIndexing.getTime() - start.getTime()) / 60000.0
				+ " total minutes");
		InexQueryServices.runInexQueries(InexQueryServices.loadInexQueries(DEFAULT_QUERY_FILE), RESULT_DIR
				+ "inex9_fold1_xml", ie.indexDirPath);
		Date endQuerying = new Date();
		System.out.println("Querying time: "
				+ (endQuerying.getTime() - endIndexing.getTime()) / 60000.0
				+ " total minutes");
	}

	public static void runSingleQuery() { // run a single query and print index
		InexExperiment ie = new InexExperiment("DESC_NAME_09_1");
		List<InexQuery> queries = InexQueryServices.loadInexQueries(QUERY_DIR
				+ "inex09/queries.csv");
		InexQuery query = queries.get(0);
		System.out.println(query);
		// List<Document> result = ie.runQuery(query);
		// for (Document doc : result.subList(0, 100)) {
		// System.out.println(doc.get(DOCNAME_ATTRIB) + " : "
		// + doc.get(TITLE_ATTRIB));
		// }
		ie.printIndexForTerm("christopher");
	}

	public static void buildPartitionedIndex() { // partitioning independent of related tuples
		List<String> allFiles = Utils.listFilesForFolder(new File(DATASET_DIR));
		Collections.shuffle(allFiles);
		int partitionNo = 10;
		ArrayList<String[]> partitions = Utils.partitionArray(
				allFiles.toArray(new String[allFiles.size()]), partitionNo);
		InexExperiment prevExperiment = null;
		for (int i = 0; i < partitionNo; i++) {
			System.out.println("iteration " + i);
			InexExperiment ie = new InexExperiment("desc_name_trec_indie_2_"
					+ i);
			Date index_t = new Date();
			System.out.println("indexing ");
			System.out.println("partition length: " + partitions.get(i).length);
			InexIndexer.buildIndex(partitions.get(i), ie.indexDirPath);
			if (prevExperiment != null) {
				System.out.println("updating index..");
				InexIndexer.updateIndex(prevExperiment.indexDirPath, ie.indexDirPath);
			}
			Date query_t = new Date();
			prevExperiment = ie;
			System.out.println("indexing time "
					+ (query_t.getTime() - index_t.getTime()) / 60000 + "mins");
			
			System.out.println("querying time "
					+ (new Date().getTime() - query_t.getTime()) / 60000
					+ "mins");
		}
	}

	public static void buildStratifiedPartitionedIndex() { // partitioning experiment with shuffling
		try {
			List<String> allFiles = Utils.listFilesForFolder(new File(
					DATASET_DIR));
			List<String> relFiles = Utils
					.readFileLines("data/query-log/inex09/relPaths_uniq.csv");
			LinkedHashSet<String> relsSet = new LinkedHashSet<String>();
			Utils.addPrefix(relFiles, DATASET_DIR);
			Collections.addAll(relsSet,
					relFiles.toArray(new String[relFiles.size()]));
			System.out.println("All files: " + allFiles.size());
			allFiles.removeAll(relsSet);
			// Utils.shuffleList(allFiles);
			System.out.println("Other files: " + allFiles.size());
			System.out.println("Rel files: " + relFiles.size());
			System.out.println("Rels Set: " + relsSet.size());
			int partitionNo = 10;
			ArrayList<String[]> relPartitions = Utils.partitionArray(
					relsSet.toArray(new String[relsSet.size()]), partitionNo);
			ArrayList<String[]> otherPartitions = Utils.partitionArray(
					allFiles.toArray(new String[allFiles.size()]), partitionNo);
			List<String[]> partitions = Utils.mergePartitions(relPartitions,
					otherPartitions);
			InexExperiment prevExperiment = null;
			for (int i = 0; i < partitionNo; i++) {
				System.out.println("iteration " + i);
				InexExperiment ie = new InexExperiment("desc_name_trec_uniq_"
						+ i);
				Date index_t = new Date();
				System.out.println("indexing ");
				System.out.println("partition length: "
						+ partitions.get(i).length);
				InexIndexer.buildIndex(partitions.get(i), ie.indexDirPath);
				if (prevExperiment != null) {
					System.out.println("updating index..");
					InexIndexer.updateIndex(prevExperiment.indexDirPath, ie.indexDirPath);
				}
				Date query_t = new Date();
				// System.out.println("running queries");
				// ie.runQueries(ie.loadQueries(QUERY_DIR +
				// "inex09/queries.csv"),
				// "desc_name");
				prevExperiment = ie;
				System.out.println("indexing time "
						+ (query_t.getTime() - index_t.getTime()) / 60000
						+ "mins");
				System.out.println("querying time "
						+ (new Date().getTime() - query_t.getTime()) / 60000
						+ "mins");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void runQueriesOnPartitionedIndex() { // running specific queries for exp1
		for (int i = 0; i < 10; i++) {
			InexExperiment ie = new InexExperiment("desc_name_trec_indie_2_"
					+ i);
			Date query_t = new Date();
			System.out.println("running queries");
			InexQueryServices.runInexQueries(
					InexQueryServices.loadInexQueries("data/query-log/inex09/queries_uniq.csv"),
					"desc_name_trec09_uniq_2_" + i + ".csv", "???");
			System.out.println("querying time "
					+ (new Date().getTime() - query_t.getTime()) / 60000
					+ "mins");
		}
	}

	public static void runTotalQueriesOnPartitionedIndex() { 
		List<InexQuery> queries = InexQueryServices.loadInexQueries("data/query-log/inex09/queries_uniq.csv");
		String[] indexPath = new String[10];
		for (int i = 0; i < 10; i++) {
			indexPath[i] = INDEX_DIR + "desc_name_trec_indie_2_" + i;
		}
		FileWriter fw = null;
		try {
			fw = new FileWriter(RESULT_DIR + "???");
			for (InexQuery queryDAO : queries) {
				for (int i = 0; i < 10; i++) {
					try (IndexReader reader = DirectoryReader.open(FSDirectory
							.open(Paths.get(indexPath[i])))) {
						IndexSearcher searcher = new IndexSearcher(reader);
						searcher.setSimilarity(new BM25Similarity());
						TopDocs topDocs = searcher.search(
								InexQueryServices.buildLuceneQuery(queryDAO.text, Experiment.TITLE_ATTRIB,
										Experiment.CONTENT_ATTRIB), 10);
						int precisionBoundry = topDocs.scoreDocs.length > 10 ? 10
								: topDocs.scoreDocs.length;
						int sum = 0;
						for (int j = 0; j < precisionBoundry; j++) {
							Document doc = searcher
									.doc(topDocs.scoreDocs[j].doc);
							String docName = doc.get(Experiment.DOCNAME_ATTRIB);
							if (queryDAO.getRelDocs().contains(docName)) {
								sum++;
							}
						}
						queryDAO.p10 = sum / 10.0;
						fw.write(queryDAO.text + ", " + queryDAO.p3 + "\n");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				fw.close();
			} catch (IOException e){
				e.printStackTrace();
			}
		}

	}

	public void printIndexForTerm(String term) {
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(this.indexDirPath)))) {
			LeafReader leafReader = SlowCompositeReaderWrapper.wrap(reader);
			Fields fields = leafReader.fields();
			Terms terms = fields.terms("title");
			TermsEnum termsEnum = terms.iterator();
			BytesRef termBF = null;
			while ((termBF = termsEnum.next()) != null) {
				String termString = termBF.utf8ToString();
				if (termString.contains(term)) {
					System.out.println(termBF.utf8ToString());
					PostingsEnum docs = termsEnum.postings(null);
					int docid;
					while ((docid = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
						System.out.println("  "
								+ reader.document(docid).get(Experiment.TITLE_ATTRIB)
								+ " "
								+ reader.document(docid).get(Experiment.DOCNAME_ATTRIB));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void printIndex() {
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(this.indexDirPath)))) {
			LeafReader leafReader = SlowCompositeReaderWrapper.wrap(reader);
			Fields fields = leafReader.fields();
			for (String field : fields) {
				System.out.println(field);
				Terms terms = fields.terms(field);
				TermsEnum termsEnum = terms.iterator();
				BytesRef termBF = null;
				while ((termBF = termsEnum.next()) != null) {
					// System.out.println(termBF);
					boolean found = termsEnum.seekExact(termBF);
					if (found) {
						PostingsEnum docs = termsEnum.postings(null);
						int docid;
						while ((docid = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
							// System.out.println(docid);
							// System.out.println(docs.freq());
							System.out.println("  "
									+ reader.document(docid).get(Experiment.TITLE_ATTRIB));
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
