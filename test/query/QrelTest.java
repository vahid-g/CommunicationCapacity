package query;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class QrelTest {

    @Test
    public void testEquals() {
	Qrel qrel1 = new Qrel(1, "qrel", 10);
	Qrel qrel2 = new Qrel(1, "qrel", 10);
	assertTrue(qrel1.equals(qrel2));
    }

    @Test
    public void testLoadQrelFile() throws IOException {
	StringBuilder qrelStringBuilder = new StringBuilder();
	qrelStringBuilder.append("1 Q0 qrel1 10\n");
	qrelStringBuilder.append("1 Q0 qrel2 1\n");
	qrelStringBuilder.append("1 Q0 qrel3 0\n");
	qrelStringBuilder.append("10 Q0 qrel1 1\n");
	InputStream stream = new ByteArrayInputStream(qrelStringBuilder
		.toString().getBytes(StandardCharsets.UTF_8.name()));
	Map<Integer, Set<Qrel>> map = Qrel.loadQrelFile(stream);

	assertTrue(map.size() == 2);
	Set<Qrel> qrelSet = new HashSet<Qrel>();
	qrelSet.add(new Qrel(1, "qrel1", 10));
	qrelSet.add(new Qrel(1, "qrel2", 1));
	assertTrue(map.get(1).equals(qrelSet));

	qrelSet = new HashSet<Qrel>();
	qrelSet.add(new Qrel(10, "qrel1", 1));
	assertTrue(map.get(10).equals(qrelSet));

	stream.close();
    }

}
