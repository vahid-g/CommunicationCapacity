package wiki13.cache_selection;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import wiki13.WikiFileIndexer;

public class BuildPopIndex {

	// Note that this code just works with flat index structures! (Indexes with
	// height = 2)
	public static void main(String[] args) {
		String indexPath = args[0]; // "/data/ghadakcv/wiki_index/1";
		String field = args[1]; // WikiFileIndexer.TITLE_ATTRIB;
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory);
				FileWriter fw = new FileWriter(indexPath + "_" + field + "_pop" + ".csv")) {
			Terms terms = MultiFields.getTerms(reader, field);
			final TermsEnum it = terms.iterator();
			BytesRef term = it.next();
			while (term != null) {
				String termString = term.utf8ToString();
				double termPopularitySum = 0;
				double termPopularityMin = Double.MAX_VALUE;
				for (LeafReaderContext lrc : reader.leaves()) {
					LeafReader lr = lrc.reader();
					if (lr.leaves().size() > 1) {
						System.out.println("\t\t" + lr.leaves().size());
					}
					PostingsEnum pe = lr.postings(new Term(field, termString));
					if (pe == null) {
						continue;
					}
					int docId = pe.nextDoc();
					double postingSize = 0;
					while (docId != PostingsEnum.NO_MORE_DOCS) {
						postingSize++;
						Document doc = lr.document(docId);
						double termDocPopularity = Double.parseDouble(doc.get(WikiFileIndexer.WEIGHT_ATTRIB));
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
				term = it.next();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
