package cache_enhancement;

import org.apache.commons.lang3.NotImplementedException;

import java.util.List;

public class UpdateDocument extends CsvParsable {
    public final String path;
    public final Integer articleId;
    public final double distance;
    public final char flag;

    private UpdateDocument(String p, Integer aId, double dist, char flg) {
        path = p;
        articleId = aId;
        distance = dist;
        flag = flg;
    }

    public static UpdateDocument get(String csvLine) {
        List<String> fields = UpdateDocument.parse(csvLine);
        String path = fields.get(0);
        Integer articleId = Integer.getInteger(fields.get(1));
        double distance = Double.parseDouble(fields.get(1));
        char flg = fields.get(1).charAt(0);
        return new UpdateDocument(path, articleId, distance, flg);
    }

    public static List<UpdateDocument> build(String csvFilePath) {
        throw new NotImplementedException("build");
    }
}
