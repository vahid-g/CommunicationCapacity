package database;

public enum DatabaseType {

	WIKIPEDIA("wiki-db"), STACKOVERFLOW("stack-db");

	private String type;

	DatabaseType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
