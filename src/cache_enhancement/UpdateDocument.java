package cache_enhancement;


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

        char flag = 'n';
        String num = "00000";
        String path = "";
        double dist = 0.0;
        if (fields.size() > 0)
            flag = fields.get(0).charAt(0);
        if (fields.size() > 1)
            num = fields.get(1);
        if (fields.size() > 2)
            path = fields.get(2);
        if (fields.size() > 3)
            dist = Double.parseDouble(fields.get(3));

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
