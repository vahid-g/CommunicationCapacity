package query;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class ExperimentQueryTest {

    @Test
    public void testAddRelevantAnswer() {
	ExperimentQuery eq = new ExperimentQuery(1, "text");
	Qrel qrel = new Qrel(1, "qrel", 8);
	eq.addRelevantAnswer(qrel);
	assertTrue(eq.hasQrelId("qrel"));
    }

    @Test
    public void testExperimentQueryConstructor() {
	Set<Qrel> qrels = new HashSet<Qrel>();
	qrels.add(new Qrel(1, "qrel1", 8));
	qrels.add(new Qrel(2, "qrel2", 1));
	ExperimentQuery eq = new ExperimentQuery(1, "text", qrels);
	assertTrue(eq.hasQrelId("qrel1"));
	assertTrue(eq.hasQrelId("qrel2"));
	assertTrue(!eq.hasQrelId(""));
    }

}
