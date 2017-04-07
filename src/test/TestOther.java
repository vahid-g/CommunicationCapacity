package test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;



public class TestOther {

	static final Logger LOGGER = Logger.getLogger(TestOther.class.getName());
	
	public static void main(String[] args) {
		List<String> list = new ArrayList<String>();
		list.add("hanhan");
		list.add("olde?");
		list.addAll(null);
		System.out.println(list);
	}
}
