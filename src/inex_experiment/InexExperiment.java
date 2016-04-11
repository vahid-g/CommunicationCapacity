package inex_experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class InexExperiment {

	public static final String CONTENT_ATTRIB = "content";
	public static final String DOCNAME_ATTRIB = "name";
	public static final String TITLE_ATTRIB = "title";
	public static final int MAX_HIT_COUNT = 100;

	public static final String INDEX_DIR = "data/index/";
	public static final String RESULT_DIR = "data/result/";
	public static final String QUERY_DIR = "data/query-log/";
	public static final String DEFAULT_QUERY_FILE = QUERY_DIR
			+ "queries_all.csv";
	public static final String DATASET_DIR = "/scratch/data-sets/INEX/";

	static int queryCoutner = 0;

	protected IndexWriter writer;
	private String indexDirPath;

	private IndexWriterConfig getConfig() {
		IndexWriterConfig config;
		config = new IndexWriterConfig(new StandardAnalyzer());
		config.setOpenMode(OpenMode.CREATE);
		config.setRAMBufferSizeMB(1024.00);
		config.setSimilarity(new BM25Similarity());
		return config;
	}

	public InexExperiment() {
		super();
	}

	public InexExperiment(String indexName) {
		indexDirPath = INDEX_DIR + indexName + "/";
	}

	public static void main(String[] args) {
		// exp1();
		exp2();
		// exp4();
	}

	public static void exp0() { // indexing and querying whole dataset
		InexExperiment ie = new InexExperiment("inex9_xml");
		Date start = new Date();
		// ie.buildIndex(INEX_DIR);
		Date endIndexing = new Date();
		ie.runQueries(ie.loadQueries(DEFAULT_QUERY_FILE), RESULT_DIR
				+ "inex9_fold1_xml");
		Date endQuerying = new Date();
		System.out.println("Indexing time: "
				+ (endIndexing.getTime() - start.getTime()) / 60000.0
				+ " total minutes");
		System.out.println("Querying time: "
				+ (endQuerying.getTime() - endIndexing.getTime()) / 60000.0
				+ " total minutes");
	}

	public static void exp1() { // partitioning experiment with shuffling
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
				ie.buildIndex(partitions.get(i));
				if (prevExperiment != null) {
					System.out.println("updating index..");
					ie.updateIndex(prevExperiment.indexDirPath);
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

	public static void exp2() { // running specific queries for exp1
		for (int i = 0; i < 10; i++) {
			InexExperiment ie = new InexExperiment("desc_name_trec_indie_2_"
					+ i);
			Date query_t = new Date();
			System.out.println("running queries");
			ie.runQueries(
					ie.loadQueries("data/query-log/inex09/queries_uniq.csv"),
					"desc_name_trec09_uniq_2_" + i + ".csv");
			System.out.println("querying time "
					+ (new Date().getTime() - query_t.getTime()) / 60000
					+ "mins");
		}
	}

	public static void exp3() { // run a single query and print index
		InexExperiment ie = new InexExperiment("DESC_NAME_09_1");
		List<QueryDAO> queries = ie.loadQueries(QUERY_DIR
				+ "inex09/queries.csv");
		QueryDAO query = queries.get(0);
		System.out.println(query);
		// List<Document> result = ie.runQuery(query);
		// for (Document doc : result.subList(0, 100)) {
		// System.out.println(doc.get(DOCNAME_ATTRIB) + " : "
		// + doc.get(TITLE_ATTRIB));
		// }
		ie.printIndexForTerm("christopher");
	}

	public static void exp4() { // partitioning independent of related tuples
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
			ie.buildIndex(partitions.get(i));
			if (prevExperiment != null) {
				System.out.println("updating index..");
				ie.updateIndex(prevExperiment.indexDirPath);
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

	public void buildIndex(String datasetFolderPath) {
		FSDirectory directory = null;
		try {
			System.out.println("indexing to: " + indexDirPath);
			directory = FSDirectory.open(Paths.get(indexDirPath));
			writer = new IndexWriter(directory, getConfig());
			this.indexFileFolder(datasetFolderPath);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (directory != null)
				directory.close();
		}
	}

	public void buildIndex(String[] datasetFilePaths) {
		FSDirectory directory = null;
		try {
			directory = FSDirectory.open(Paths.get(indexDirPath));
			writer = new IndexWriter(directory, getConfig());
			for (String filePath : datasetFilePaths) {
				this.indexFile(new File(filePath));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (directory != null)
				directory.close();
		}
	}

	public void updateIndex(String newIndexPath) {
		String tmpIndexPath = INDEX_DIR + "tmp_index";
		FSDirectory newIndexDir = null;
		FSDirectory tmpDir = null;
		FSDirectory currentDir = null;
		IndexWriter writer = null;
		try {
			newIndexDir = FSDirectory.open(Paths.get(newIndexPath));
			tmpDir = FSDirectory.open(Paths.get(tmpIndexPath));
			currentDir = FSDirectory.open(Paths.get(indexDirPath));
			writer = new IndexWriter(tmpDir, getConfig());
			writer.addIndexes(newIndexDir, currentDir);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (tmpDir != null)
				tmpDir.close();
			if (newIndexDir != null)
				newIndexDir.close();
			if (currentDir != null)
				currentDir.close();
		}
		// housekeeping
		try {
			File currentIndex = new File(indexDirPath);
			FileUtils.deleteDirectory(currentIndex);
			File newIndex = new File(tmpIndexPath);
			newIndex.renameTo(new File(indexDirPath));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void mergeIndices(String[] inputIndexPaths) {
		FSDirectory writeDir = null;
		FSDirectory[] inputDirs = new FSDirectory[inputIndexPaths.length];
		IndexWriter writer = null;
		try {
			writeDir = FSDirectory.open(Paths.get(this.indexDirPath));
			writer = new IndexWriter(writeDir, getConfig());
			for (int i = 0; i < inputIndexPaths.length; i++) {
				System.out.println(inputIndexPaths[i]);
				inputDirs[i] = FSDirectory.open(Paths.get(INDEX_DIR
						+ inputIndexPaths[i]));
			}
			writer.addIndexes(inputDirs);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (writeDir != null)
				writeDir.close();
			for (FSDirectory fsd : inputDirs) {
				if (fsd != null)
					fsd.close();
			}
		}
	}

	protected void indexFileFolder(String filePath) {
		File file = new File(filePath);
		if (!file.exists()) {
			System.out.println("File " + file.getAbsolutePath()
					+ " does not exist!");
			return;
		} else {
			if (file.isDirectory()) {
				System.out.println(" indexing dir " + file.getPath());
				for (File f : file.listFiles()) {
					indexFileFolder(f.getAbsolutePath());
				}
			} else { // file is not a directory
				indexFile(file);
			}
		}
	}

	protected void indexFile(File file) {
		try (InputStream fis = Files.newInputStream(file.toPath())) {
			byte[] data = new byte[(int) file.length()];
			fis.read(data);
			String fileContent = new String(data, "UTF-8");
			int length = fileContent.length() > 8 ? 8 : fileContent.length();
			if (fileContent.substring(0, length).equals("REDIRECT")) {
				return;
			}
			Pattern p = Pattern.compile(".*<title>(.*?)</title>.*",
					Pattern.DOTALL);
			Matcher m = p.matcher(fileContent);
			m.find();
			String title = "";
			if (m.matches())
				title = m.group(1);
			else
				System.out.println("!!! title not found in " + file.getName());
			fileContent = fileContent.replaceAll("\\<.*?\\>", " ");
			Document doc = new Document();
			doc.add(new StringField(DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			doc.add(new TextField(TITLE_ATTRIB, title, Field.Store.YES));
			doc.add(new TextField(CONTENT_ATTRIB, fileContent, Field.Store.YES));
			// doc.add(new TextField(CONTENT_ATTRIB, new BufferedReader(
			// new InputStreamReader(fis, StandardCharsets.UTF_8))));
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void runUniQueries(List<QueryDAO> queries, String resultFileName) {
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(this.indexDirPath)))) {
			System.out.println("Number of docs in index: " + reader.numDocs());
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(new BM25Similarity());
			for (QueryDAO queryDAO : queries) {
				// System.out.println(queryCoutner++);
				TopDocs topDocs = searcher
						.search(buildQuery(queryDAO.text, TITLE_ATTRIB,
								CONTENT_ATTRIB), MAX_HIT_COUNT);
				for (int i = 0; i < topDocs.scoreDocs.length; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					if (doc.get(DOCNAME_ATTRIB).equals(
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
					if (doc.get(DOCNAME_ATTRIB).equals(
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
		try (FileWriter fw = new FileWriter(RESULT_DIR + resultFileName)) {
			for (QueryDAO query : queries) {
				fw.write(query.text + ", " + query.p3 + ", " + query.p10 + ", "
						+ query.mrr + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void runQueries(List<QueryDAO> queries, String resultFileName) {
		try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(this.indexDirPath)))) {
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
			for (QueryDAO queryDAO : queries) {
				// System.out.println(queryCoutner++);
				TopDocs topDocs = searcher
						.search(buildQuery(queryDAO.text, TITLE_ATTRIB,
								CONTENT_ATTRIB), 10);
				int precisionBoundry = topDocs.scoreDocs.length > 10 ? 10
						: topDocs.scoreDocs.length;
				int sum = 0;
				for (int i = 0; i < precisionBoundry; i++) {
					Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
					String docName = doc.get(DOCNAME_ATTRIB);

					if (queryDAO.getRelDocs().contains(docName)) {
						sum++;
					}
				}
				queryDAO.p10 = sum / 10.0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (FileWriter fw = new FileWriter(RESULT_DIR + resultFileName)) {
			for (QueryDAO query : queries) {
				fw.write(query.text + ", " + query.p3 + ", " + query.p10 + ", "
						+ query.mrr + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected Query buildQuery(String queryString, String field) {
		QueryParser parser = new QueryParser(field, new StandardAnalyzer());
		Query query = null;
		try {
			query = parser.parse(QueryParser.escape(queryString));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return query;
	}

	protected Query buildQuery(String queryString, String... fields) {
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
		QueryParser nameParser = new QueryParser(DOCNAME_ATTRIB,
				new StandardAnalyzer());
		Query nameQuery;
		Query contentQuery;
		try {
			nameQuery = nameParser.parse(QueryParser.escape(queryText));
			QueryParser contentParser = new QueryParser(CONTENT_ATTRIB,
					new StandardAnalyzer());
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

	public List<QueryDAO> loadQueries(String queryFile) {
		List<QueryDAO> queries = new ArrayList<QueryDAO>();
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
					queries.add(new QueryDAO(queryText, rel));
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
								+ reader.document(docid).get(TITLE_ATTRIB)
								+ " "
								+ reader.document(docid).get(DOCNAME_ATTRIB));
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
									+ reader.document(docid).get(TITLE_ATTRIB));
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
