package cache_enhancement;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CsvParsable {
    public static void main(String args[]){
        System.out.println(parse("1 ;2 ;3; 4; 5 ; 6 ;7", ";", 5));
        System.out.println(parse("1 ;2 ;3; 4; 5 ; 6 ;7", ";", 5, false));
    }

    public static List<String> parse(String csvTextLine, String separator, int limit) {
        final String regex = "\\s*" + separator + "\\s*";
        return new ArrayList<String>(Arrays.asList(csvTextLine.trim().split(regex, limit)));
    }

    public static List<String> parse(String csvTextLine, String separator) {
        return parse(csvTextLine, separator, 0);
    }

    public static List<String> parse(String csvTextLine) {
        return parse(csvTextLine, ",", 0);
    }

    public static List<String> parse(String csvTextLine, String separator, int limit, boolean reverse) {
        List<String> parsed = new ArrayList<String>();
        if (limit <= 1) {
            return parse(csvTextLine, separator, limit);
        }

        int pos = ordinalIndexOf(csvTextLine, separator, limit-1, reverse);
        if (pos == -1){
            parsed = parse(csvTextLine, separator, limit);
        }
        else if (reverse){
            String firstPart = csvTextLine.substring(0, pos).trim();
            parsed = parse(csvTextLine.substring(pos+1), separator, limit-1);
            parsed.add(0, firstPart);
        }
        else if (!reverse){
            String lastPart = csvTextLine.substring(pos+1).trim();
            parsed = parse(csvTextLine.substring(0, pos), separator, limit-1);
            parsed.add(lastPart);
        }
        return parsed;
    }

    /**
    * https://programming.guide/java/nth-occurrence-in-string.html
     */
    public static int ordinalIndexOf(String str, String substr, int n, boolean reverse) {
        if(!reverse) {
            int pos = str.indexOf(substr);
            while (--n > 0 && pos != -1)
                pos = str.indexOf(substr, pos + 1);
            return pos;
        }
        else {
            int pos = str.lastIndexOf(substr);
            while (--n > 0 && pos != -1)
                pos = str.lastIndexOf(substr, pos - 1);
            return pos;
        }
    }

    public static List<List<String>> parseFile(String csvFilePath, String separator) throws IOException {
        List<List<String>> csvArray = new ArrayList<>();
        File csvFile = new File(csvFilePath);
        FileReader fileReader = new FileReader(csvFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        while((line = bufferedReader.readLine()) != null) {
            csvArray.add(parse(line, separator));
        }
        return csvArray;
    }

    public static List<List<String>> parseFile(String csvFilePath, String separator,
                                               int limit, boolean reverse) throws IOException {
        List<List<String>> csvArray = new ArrayList<>();
        File csvFile = new File(csvFilePath);
        FileReader fileReader = new FileReader(csvFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        while((line = bufferedReader.readLine()) != null) {
            csvArray.add(parse(line, separator, limit, reverse));
        }
        return csvArray;
    }

    public static List<List<String>> parseFile(String csvFilePath) throws IOException {return parseFile(csvFilePath, ",");}
}
