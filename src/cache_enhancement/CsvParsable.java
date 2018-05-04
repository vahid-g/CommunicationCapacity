package cache_enhancement;

import java.util.List;

abstract public class CsvParsable {
    protected static List<String> parse(String csvLine, char seperator) {
        List<String> x = null;
        return x;
    }

    protected static List<String> parse(String csvLine) {
        return parse(csvLine, ',');
    }
}
