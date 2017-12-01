package query;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Qrel {

    static final Logger LOGGER = Logger.getLogger(Qrel.class.getName());

    public static Map<Integer, Set<Qrel>> loadQrelFile(
	    InputStream qrelInputStream) {
	Map<Integer, Set<Qrel>> qidQrels = new HashMap<Integer, Set<Qrel>>();
	try (Scanner sc = new Scanner(qrelInputStream)) {
	    String line;
	    while (sc.hasNextLine()) {
		line = sc.nextLine();
		Pattern ptr = Pattern
			.compile("(\\d+)\\sQ?0\\s(\\w+)\\s([0-9]+)");
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
	}
	return qidQrels;
    }

    private int qid;
    private String qrelId;
    private int rel;

    public Qrel(int qid, String qrelId, int rel) {
	this.qid = qid;
	this.qrelId = qrelId;
	this.rel = rel;
    }

    @Override
    public boolean equals(Object obj) {
	if (obj instanceof Qrel) {
	    Qrel toCompare = (Qrel) obj;
	    return ((toCompare.qid == this.qid)
		    && toCompare.qrelId.equals(this.getQrelId()) && toCompare.rel == this.rel);
	}
	return false;
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

    @Override
    public int hashCode() {
	return this.toString().hashCode();
    }

    @Override
    public String toString() {
	return qid + " Q0 " + qrelId + " " + rel;
    }

}
