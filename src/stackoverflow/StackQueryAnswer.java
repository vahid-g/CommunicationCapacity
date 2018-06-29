package stackoverflow;

public class StackQueryAnswer {
	
	QuestionDAO question;
	public double rrank = 0;
	public double recall = 0;
	public double p20 = 0;
	

	public StackQueryAnswer(QuestionDAO question) {
		this.question = question;
	}
	
}
