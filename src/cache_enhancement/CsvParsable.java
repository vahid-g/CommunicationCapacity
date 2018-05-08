package cache_enhancement;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

abstract public class CsvParsable {
    protected static List<String> parse(String csvTextLine, String seperator) {
        final String regex = "\\s*" + seperator + "\\s*";
        return new ArrayList<String>(Arrays.asList(csvTextLine.trim().split(regex)));
    }

    protected static List<String> parse(String csvTextLine) {
        return parse(csvTextLine, ",");
    }

    public static List<List<String>> parseFile(String csvFilePath, String seperator) throws IOException {
        List<List<String>> csvArray = new ArrayList<>();
        File csvFile = new File(csvFilePath);
        FileReader fileReader = new FileReader(csvFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        while((line = bufferedReader.readLine()) != null) {
            csvArray.add(parse(line, seperator));
        }

        return csvArray;
    }
    public static List<List<String>> parseFile(String csvFilePath) throws IOException {return parseFile(csvFilePath, ",");}
}
