package stackoverflow;

import java.util.ArrayList;
import java.util.List;

public class StackQueryAnswer {
	
	QuestionDAO question;
	public double rrank = 0;
	public double recall = 0;
	

	public StackQueryAnswer(QuestionDAO question) {
		this.question = question;
	}
	
}
