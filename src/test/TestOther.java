package test;

import java.util.ArrayList;
import java.util.List;

import inex09.InexQuery;
import inex09.InexQueryResult;

public class TestOther {
	public static void main(String[] args) {
		List<InexQueryResult> list = new ArrayList<InexQueryResult>();
		list.add(new InexQueryResult(new InexQuery("1", "olde")));
		list.add(new InexQueryResult(new InexQuery("2", "olde")));
		list.add(new InexQueryResult(new InexQuery("3", "olde")));
		System.out.println(list.toString());
	}
}
