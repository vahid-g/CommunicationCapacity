package query;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Qrel {

	static final Logger LOGGER = Logger.getLogger(Qrel.class.getName());

	private int qid;
	private String qrelId;
	private int rel;

	public Qrel(int qid, String qrelId, int rel) {
		this.qid = qid;
		this.qrelId = qrelId;
		this.rel = rel;
	}

	public int getQid() {
		return qid;
	}
	
	public String getQrelId() {
		return qrelId;
	}

	public int getRel() {
		return rel;
	}

	// This method is similar to QueryServices.loadQrels except that it build
	// Qrel objects from the input file and includes the relevancy score of
	// the qrels as well
	public static HashMap<Integer, Set<Qrel>> loadQrelFile(String path) {
		HashMap<Integer, Set<Qrel>> qidQrels = new HashMap<Integer, Set<Qrel>>();
		try (Scanner sc = new Scanner(new File(path))) {
			String line;
			while (sc.hasNextLine()) {
				line = sc.nextLine();
				Pattern ptr = Pattern.compile("(\\d+)\\sQ?0\\s(\\w+)\\s([0-9])");
				Matcher m = ptr.matcher(line);
				if (m.find()) {
					if (!m.group(3).equals("0")) {
						Integer qid = Integer.parseInt(m.group(1));
						String qrelId = m.group(2);
						Integer rel = Integer.parseInt(m.group(3));
						Set<Qrel> qrels = qidQrels.get(qid);
						if (qrels == null) {
							qrels = new HashSet<Qrel>();
							qrels.add(new Qrel(qid, qrelId, rel));
							qidQrels.put(qid, qrels);
						} else {
							qrels.add(new Qrel(qid, qrelId, rel));
						}
					}
				} else {
					LOGGER.log(Level.WARNING, "regex failed for line: " + line);
				}
			}
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, "QREL file not found!");
		}
		return qidQrels;
	}

}
