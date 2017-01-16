package inex09;

import inex13.Experiment;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

public class InexIndexer{

	static IndexWriterConfig getConfig(boolean append) {
		IndexWriterConfig config;
		config = new IndexWriterConfig(new StandardAnalyzer());
		if (append)
			config.setOpenMode(OpenMode.APPEND);
		else
			config.setOpenMode(OpenMode.CREATE);
		config.setRAMBufferSizeMB(1024.00);
		config.setSimilarity(new BM25Similarity());
		return config;
	}

	public static void buildIndex(String datasetFolderPath, String indexPath) {
		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			System.out.println("indexing to: " + indexPath);
			directory = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(directory, getConfig(false));
			indexFileFolder(datasetFolderPath, writer);
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

	public static void buildIndex(String[] datasetFilePaths, String indexPath) {
		buildIndex(datasetFilePaths, indexPath, true);
	}

	public static void buildIndex(String[] datasetFilePaths, String indexPath,
			boolean isXml) {
		FSDirectory directory = null;
		IndexWriter writer = null;
		try {
			directory = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(directory, getConfig(false));
			for (String filePath : datasetFilePaths) {
				if (isXml)
					indexXmlFile(filePath, writer);
				else
					indexFile(filePath, writer);
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

	public static void updateIndex(String prevIndexPath, String indexPath) {
		FSDirectory prevIndexDir = null;
		FSDirectory currentDir = null;
		IndexWriter writer = null;
		try {
			prevIndexDir = FSDirectory.open(Paths.get(prevIndexPath));
			currentDir = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(currentDir, getConfig(true));
			writer.addIndexes(prevIndexDir);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (prevIndexDir != null)
				prevIndexDir.close();
			if (currentDir != null)
				currentDir.close();
		}
	}

	static void mergeIndices(String[] inputIndexPaths, String indexPath) {
		FSDirectory writeDir = null;
		FSDirectory[] inputDirs = new FSDirectory[inputIndexPaths.length];
		IndexWriter writer = null;
		try {
			writeDir = FSDirectory.open(Paths.get(indexPath));
			writer = new IndexWriter(writeDir, getConfig(false));
			for (int i = 0; i < inputIndexPaths.length; i++) {
				System.out.println(inputIndexPaths[i]);
				inputDirs[i] = FSDirectory.open(Paths
						.get(InexExperiment.INDEX_DIR + inputIndexPaths[i]));
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

	static void indexFileFolder(String filePath, IndexWriter writer) {
		File file = new File(filePath);
		if (!file.exists()) {
			System.out.println("File " + file.getAbsolutePath()
					+ " does not exist!");
			return;
		} else {
			if (file.isDirectory()) {
				System.out.println(" indexing dir " + file.getPath());
				for (File f : file.listFiles()) {
					indexFileFolder(f.getAbsolutePath(), writer);
				}
			} else { // file is not a directory
				indexXmlFile(file, writer);
			}
		}
	}

	static void indexXmlFile(String filePath, IndexWriter writer) {
		indexXmlFile(new File(filePath), writer);
	}

	static void indexXmlFile(File file, IndexWriter writer) {
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
			doc.add(new StringField(Experiment.DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			doc.add(new TextField(Experiment.TITLE_ATTRIB, title,
					Field.Store.YES));
			doc.add(new TextField(Experiment.CONTENT_ATTRIB, fileContent,
					Field.Store.YES));
			// doc.add(new TextField(CONTENT_ATTRIB, new BufferedReader(
			// new InputStreamReader(fis, StandardCharsets.UTF_8))));
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void indexFile(String filepath, IndexWriter writer) {
		File file = new File(filepath);
		//try (InputStream is = Files.newInputStream(file.toPath())) {
//		try (InputStream is =  new BufferedInputStream(new FileInputStream(file))) {
		try{
//			byte[] data = new byte[(int) file.length()];
//			is.read(data);
//			String fileContent = new String(data, "UTF-8");
			String fileContent = new String(Files.readAllBytes(Paths.get(filepath)), StandardCharsets.UTF_8);
			if (isRedirectingFile(fileContent))
				return;
			Document doc = new Document();
			doc.add(new StringField(Experiment.DOCNAME_ATTRIB, FilenameUtils
					.removeExtension(file.getName()), Field.Store.YES));
			doc.add(new TextField(Experiment.CONTENT_ATTRIB, fileContent,
					Field.Store.YES));
			writer.addDocument(doc);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean isRedirectingFile(String fileContent) {
		int length = fileContent.length() > 8 ? 8 : fileContent.length();
		return (fileContent.substring(0, length).equals("REDIRECT"));
	}

}
