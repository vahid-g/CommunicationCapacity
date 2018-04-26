package indexing.popularity;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import indexing.BiwordAnalyzer;

public class BuildPopIndexFast {

	public static final Logger LOGGER = Logger.getLogger(BuildPopIndexSlow.class.getName());

	public static void main(String[] args) {
		String indexPath = args[0]; // "/data/ghadakcv/wiki_index/1";
		String field = args[1]; // WikiFileIndexer.TITLE_ATTRIB;
		String weightFieldName = args[2]; // WikiFileIndexer.WEIGHT_ATTRIB
		if (Arrays.asList(args).contains("-parallel")) {
			LOGGER.log(Level.INFO, "parallel..");
			parallelBuildPopIndex(indexPath, field, weightFieldName);
		} else if (Arrays.asList(args).contains("-small")) {
			LOGGER.log(Level.INFO, "small..");
			Set<String> tokens = buildBiwordsOfQueries("/data/ghadakcv/msn_vocab.csv");
			buildPopIndexForTokens(indexPath, field, weightFieldName, tokens);
		} else {
			buildPopIndex(indexPath, field, weightFieldName);
		}
	}

	public static void buildPopIndex(String indexPath, String field, String weightFieldName) {
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory);
				FileWriter fw = new FileWriter(indexPath + "_" + field + "_pop_fast" + ".csv")) {
			Terms terms = MultiFields.getTerms(reader, field);
			final TermsEnum it = terms.iterator();
			int counter = 0;
			while (it.next() != null) {
				BytesRef term = it.term();
				String termString = term.utf8ToString();
				if (++counter % 10000 == 0) {
					LOGGER.log(Level.INFO, "counter = " + counter);
					LOGGER.log(Level.INFO, termString);
				}
				double termPopularitySum = 0;
				double termPopularityMin = Double.MAX_VALUE;
				PostingsEnum pe = it.postings(null);
				double postingSize = 0;
				int docId = 0;
				while ((docId = pe.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
					postingSize++;
					Document doc = reader.document(docId);
					double termDocPopularity = Double.parseDouble(doc.get(weightFieldName));
					termPopularitySum += termDocPopularity;
					termPopularityMin = Math.min(termDocPopularity, termPopularityMin);
					docId = pe.nextDoc();
				}
				double termPopularityMean = 0;
				if (postingSize != 0) {
					termPopularityMean = termPopularitySum / postingSize;
				}
				fw.write(termString + "," + termPopularityMean + "," + termPopularityMin + "\n");
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public static void buildPopIndexForTokens(String indexPath, String field, String weightFieldName,
			Set<String> tokens) {
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory);
				FileWriter fw = new FileWriter(indexPath + "_" + field + "_pop_fast_tokens" + ".csv")) {
			Terms terms = MultiFields.getTerms(reader, field);
			final TermsEnum it = terms.iterator();
			int counter = 0;
			while (it.next() != null) {
				BytesRef term = it.term();
				String termString = term.utf8ToString();
				if (tokens.contains(termString)) {
					if (++counter % 10000 == 0) {
						LOGGER.log(Level.INFO, "counter = " + counter);
						LOGGER.log(Level.INFO, termString);
					}
					double termPopularitySum = 0;
					double termPopularityMin = Double.MAX_VALUE;
					PostingsEnum pe = it.postings(null);
					double postingSize = 0;
					int docId = 0;
					while ((docId = pe.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
						postingSize++;
						Document doc = reader.document(docId);
						double termDocPopularity = Double.parseDouble(doc.get(weightFieldName));
						termPopularitySum += termDocPopularity;
						termPopularityMin = Math.min(termDocPopularity, termPopularityMin);
						docId = pe.nextDoc();
					}
					double termPopularityMean = 0;
					if (postingSize != 0) {
						termPopularityMean = termPopularitySum / postingSize;
					}
					fw.write(termString + "," + termPopularityMean + "," + termPopularityMin + "\n");
				}
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private static Set<String> buildBiwordsOfQueries(String vocabFile) {
		Set<String> tokenSet = new HashSet<String>();
		try (Analyzer analyzer = new BiwordAnalyzer()) {
			try (TokenStream tokenStream = analyzer.tokenStream("f", new FileReader(vocabFile))) {
				CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
				tokenStream.reset();
				while (tokenStream.incrementToken()) {
					String biword = termAtt.toString();
					tokenSet.add(biword);
				}
				tokenStream.end();
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		return tokenSet;
	}

	public static void parallelBuildPopIndex(String indexPath, String field, String weightFieldName) {
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory);
				FileWriter fw = new FileWriter(indexPath + "_" + field + "_pop_veryfast" + ".csv")) {
			Terms terms = MultiFields.getTerms(reader, field);
			final TermsEnum it = terms.iterator();
			List<String> termList = new ArrayList<String>();
			while (it.next() != null) {
				termList.add(it.term().utf8ToString());
			}
			List<TokenPopularity> tokenPopularityList = termList.parallelStream().map(termEnum -> {
				double termPopularitySum = 0;
				double termPopularityMin = Double.MAX_VALUE;
				double termPopularityMean = 0;
				double postingSize = 0;
				int docId = 0;
				PostingsEnum pe;
				try {
					pe = it.postings(null);
					while ((docId = pe.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
						postingSize++;
						Document doc = reader.document(docId);
						double termDocPopularity = Double.parseDouble(doc.get(weightFieldName));
						termPopularitySum += termDocPopularity;
						termPopularityMin = Math.min(termDocPopularity, termPopularityMin);
						docId = pe.nextDoc();
					}
					if (postingSize != 0) {
						termPopularityMean = termPopularitySum / postingSize;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				return new TokenPopularity(termPopularityMean, termPopularityMin);
			}).collect(Collectors.toList());
			for (int i = 0; i < termList.size(); i++) {
				fw.write(termList.get(i) + "," + tokenPopularityList.get(i).mean + "," + tokenPopularityList.get(i).min
						+ "\n");
			}
		} catch (

		IOException e) {
			e.printStackTrace();
		}
	}

}
