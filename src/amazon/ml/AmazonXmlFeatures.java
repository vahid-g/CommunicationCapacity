package amazon.ml;

public enum AmazonXmlFeatures {
    BINDING("binding"), PRICE("listprice"), DATE("publicationdate"), DEWEY(
	    "dewey"), PAGES("numberofpages"), IMAGES("image"), REVIEWS("review"), EDITOR_REVIEWS(
	    "editorialreview"), CREATORS("creator"), SIMILAR_PRODS(
	    "similarproduct"), BROWSE_NODES("browseNode");

    private final String label;

    AmazonXmlFeatures(final String s) {
	label = s;
    }

    public String toString() {
	return label;
    }
}
