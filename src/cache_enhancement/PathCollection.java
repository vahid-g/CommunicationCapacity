package cache_enhancement;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PathCollection {
    public final String timestamp = new SimpleDateFormat("MMddHHmm").format(new Date());
    private static PathCollection myInstance = null;
    private static String pathGroup = "";

    static public PathCollection get(String group) {
        if (!pathGroup.equals(group)) {
            pathGroup = group;
            myInstance = new PathCollection(group);
        }
        return myInstance;
    }

    private PathCollection(String group) {
        String relativePath = "";
        switch (group) {
            case "maple":
                relativePath = "/data/khodadaa/lucene-index/";
                break;
            case "local":
                relativePath = "data/";
                break;
            case "distributed":
                allWiki13IndexPath = "some custom path";
                com2Wiki13IndexPath = "some custom path";
                sub2Wiki13IndexPath = "some custom path";
                wiki13Count13Path = "some custom path";
                msnQueryPath = "some custom path";
                msnQrelPath = "some custom path";
                defaultSavePath = "some custom path";
                return;
        }

        allWiki13IndexPath = relativePath + "100";
        com2Wiki13IndexPath = relativePath + "c2";
        sub2Wiki13IndexPath = relativePath + "2";
        wiki13Count13Path = relativePath + "wiki13_counts13_title.csv";
        msnQueryPath = relativePath + "msn_query_qid.csv";
        msnQrelPath = relativePath + "msn.qrels";
        defaultSavePath = relativePath + timestamp;
    }

    public final String allWiki13IndexPath;
    public final String com2Wiki13IndexPath;
    public final String sub2Wiki13IndexPath;
    public final String wiki13Count13Path;
    public final String msnQueryPath;
    public final String msnQrelPath;
    public final String defaultSavePath;
}
