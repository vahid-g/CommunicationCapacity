package amazon;

public enum AmazonDocumentField {
	TITLE("title"), CONTENT("content"), CREATORS("creators"), TAGS("tags"), DEWEY("dewey");

	// private static final Logger LOGGER = Logger.getLogger(AmazonDocumentField.class.getName());

	private final String label;
	
	AmazonDocumentField(final String s) {
		label = s;
	}

	public String toString() {
		return label;
	}


}
