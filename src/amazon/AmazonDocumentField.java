package amazon;

public enum AmazonDocumentField {
	TITLE("title"), CONTENT("content"), CREATOR("creator"), TAGS("tags"), DEWEY("dewey");
	
	private final String label;
	
	AmazonDocumentField(final String s){
		label = s;
	}
	
	public String toString(){
		return label;
	}
	
	public static void main(String[] args) {
		for (AmazonDocumentField docField : AmazonDocumentField.values()){
			System.out.println(docField);
		}
	}
}
