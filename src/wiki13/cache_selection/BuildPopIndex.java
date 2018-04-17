package wiki13.cache_selection;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import wiki13.WikiFileIndexer;

public class BuildPopIndex {

	public static void main(String[] args) {
		String indexPath = "/data/ghadakcv/wiki_index/1";
		try (FSDirectory directory = FSDirectory.open(Paths.get(indexPath));
				IndexReader reader = DirectoryReader.open(directory)) {
			String field = WikiFileIndexer.TITLE_ATTRIB;
			Terms terms = MultiFields.getTerms(reader, field);
			final TermsEnum it = terms.iterator();
			BytesRef term = it.next();
			int counter = 0;
			while (term != null) {
				String termString = term.utf8ToString();
				double termPopoularitySum = 0;
				double termPopularityMin = Double.MAX_VALUE;
				for (LeafReaderContext lrc : reader.leaves()) {
					LeafReader lr = lrc.reader();
					if (lr.leaves().size() > 1) {
						System.out.println(termString + ": " + lr.leaves().size());
					}
					/*
					PostingsEnum pe = lr.postings(new Term(field, termString));
					if (pe == null) {
						continue;
					}
					int docId = pe.nextDoc();
					while (docId != PostingsEnum.NO_MORE_DOCS) {
						Document doc = lr.document(docId);
						double termDocPopularity = Double.parseDouble(doc.get(WikiFileIndexer.WEIGHT_ATTRIB));
						termPopoularitySum += termDocPopularity;
						termPopularityMin = Math.min(termDocPopularity, termPopularityMin);
						docId = pe.nextDoc();
					}
					// output termString, meanPopularity, minPopularity
					 * 
					 */
				}
				term = it.next();
				if (counter++ > 10)
					break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
