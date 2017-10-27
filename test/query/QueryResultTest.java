package query;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class QueryResultTest {
	
	private static final double epsilon = 0.001;

	@Test
	public void test() {
		Set<Qrel> qrels = new HashSet<Qrel>();
		ExperimentQuery eq = new ExperimentQuery(1, "query", qrels);
		QueryResult qr = new QueryResult(eq);
		assertEquals(qr.idcg(10), 0, epsilon);
		assertEquals(qr.ndcg(10), 0, epsilon);
		qrels.add(new Qrel(1, "qrel1", 10));
		assertEquals(qr.idcg(10), 10, epsilon);
		assertEquals(qr.ndcg(10), 0, epsilon);
		qr.addResult("qrel2", "title2");
		assertEquals(qr.idcg(10), 10, epsilon);
		assertEquals(qr.ndcg(10), 0, epsilon);
		qr.addResult("qrel1", "title1");
		assertEquals(qr.idcg(10), 10, epsilon);
		assertEquals(qr.ndcg(10), 1/(Math.log(3)/Math.log(2)), epsilon);
	}

}
