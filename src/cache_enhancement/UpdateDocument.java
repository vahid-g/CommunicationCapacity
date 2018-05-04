package cache_enhancement;

import java.util.List;

public class UpdateDocument extends CsvParsable {
    public final String path;
    public final long articleId;
    public final double distance;
    public final Boolean delFlag;

    private UpdateDocument(String p, long aId, double dist, Boolean dFg) {
        path = p;
        articleId = aId;
        distance = dist;
        delFlag = dFg;
    }

    public static UpdateDocument get(String csvLine) {
        List<String> fields = UpdateDocument.parse(csvLine);
        String path = fields.get(0);
        long articleId = Integer.getInteger(fields.get(1));
        double distance = Double.parseDouble(fields.get(1));
        Boolean delFlag = Boolean.parseBoolean(fields.get(1));
        return new UpdateDocument(path, articleId, distance, delFlag);
    }
}
