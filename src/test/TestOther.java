package test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import wiki_inex09.InexQuery;
import wiki_inex09.InexQueryResult;



public class TestOther {

	static final Logger LOGGER = Logger.getLogger(TestOther.class.getName());
	
	public static void main(String[] args) {
		LOGGER.log(Level.INFO, "hanhan");
		LOGGER.log(Level.SEVERE, "olde?");
		List<InexQueryResult> list = new ArrayList<InexQueryResult>();
		list.add(new InexQueryResult(new InexQuery("1", "olde")));
		list.add(new InexQueryResult(new InexQuery("2", "olde")));
		list.add(new InexQueryResult(new InexQuery("3", "olde")));
		System.out.println(list.toString());
	}
}
