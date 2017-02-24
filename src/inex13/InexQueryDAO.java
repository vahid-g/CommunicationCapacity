package inex13;

import java.util.ArrayList;
import java.util.List;

public class InexQueryDAO {
	InexQueryDAO(int id, String text) {
		this.id = id;
		this.text = text;
	}
	int id;
	public String text;
	List<Integer> relDocIds = new ArrayList<Integer>();
}