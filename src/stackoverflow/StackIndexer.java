package stackoverflow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class StackIndexer {

	static Logger LOGGER = Logger.getLogger(StackIndexer.class.getName());

	public static void main(String[] args) throws IOException, SQLException {
		DatabaseMediator dm = new DatabaseMediator();
		// List<QuestionDAO> questions = dm.loadQuestions();
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		config.setSimilarity(new BM25Similarity());
		config.setRAMBufferSizeMB(1024);
		File indexFolder = new File("/data/ghadakcv/stack_index");
		if (!indexFolder.exists()) {
			indexFolder.mkdir();
		}
		Directory directory = FSDirectory.open(Paths.get("/data/ghadakcv"));
		try (IndexWriter iwriter = new IndexWriter(directory, config)) {
			List<AnswerDAO> answers = dm.loadAnswers();
			for (AnswerDAO answer : answers) {
				Document doc = new Document();
				doc.add(new StoredField("id", answer.id));
				doc.add(new TextField("body", answer.body, Store.NO));
			}
		}
	}

}
