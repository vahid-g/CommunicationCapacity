package inex13;

import java.util.ArrayList;
import java.util.List;

class InexQueryDAO {
	InexQueryDAO(int id, String text) {
		this.id = id;
		this.text = text;
	}
	int id;
	String text;
	List<Integer> relDocIds = new ArrayList<Integer>();
}