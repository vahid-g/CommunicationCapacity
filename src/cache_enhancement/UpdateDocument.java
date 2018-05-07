package cache_enhancement;

import org.apache.commons.lang3.NotImplementedException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UpdateDocument extends CsvParsable {
    public final char changeFlag;
    public final String docNumber;
    public final String docPath;
    public final double distance;

    private UpdateDocument(char flag, String num, String path, double dist) {
        changeFlag = flag;
        docNumber = num;
        docPath = path;
        distance = dist;
    }

    public static UpdateDocument get(String csvLine) {
        List<String> fields = UpdateDocument.parse(csvLine);
        if (fields.size() < 4){
            throw new NoSuchFieldError("CSV file is in the wrong format.");
        }

        final char flag = fields.get(0).charAt(0);
        final String num = fields.get(1);
        final String path = fields.get(2);
        final double dist = Double.parseDouble(fields.get(3));
        return new UpdateDocument(flag, num, path, dist);
    }

    public static List<UpdateDocument> buildFromFile(String csvFilePath) throws IOException {
        List<UpdateDocument> updateDocuments = new ArrayList<>();
        File csvFile = new File(csvFilePath);
        FileReader fileReader = new FileReader(csvFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        while((line = bufferedReader.readLine()) != null) {
            updateDocuments.add(get(line));
        }

        return updateDocuments;
    }
}
