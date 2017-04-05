package test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import query.ExperimentQuery;
import query.QueryResult;



public class TestOther {

	static final Logger LOGGER = Logger.getLogger(TestOther.class.getName());
	
	public static void main(String[] args) {
		LOGGER.log(Level.INFO, "hanhan");
		LOGGER.log(Level.SEVERE, "olde?");
		List<QueryResult> list = new ArrayList<QueryResult>();
		list.add(new QueryResult(new ExperimentQuery("1", "olde")));
		list.add(new QueryResult(new ExperimentQuery("2", "olde")));
		list.add(new QueryResult(new ExperimentQuery("3", "olde")));
		System.out.println(list.toString());
	}
}
