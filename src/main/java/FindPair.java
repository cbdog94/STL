import bean.Cell;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FindPair {
    public static void main(String[] args) throws IOException {
        File startFile = FileUtils.getFile("file/start.txt");
        List<String> startLines = FileUtils.readLines(startFile);
        Map<String, Integer> startMap = new HashMap<>();
        for (String line : startLines) {
            String[] splits = line.split("\t");
            if (Integer.parseInt(splits[1]) > 1000)
                startMap.put(splits[0], Integer.parseInt(splits[1]));
        }

        File endFile = FileUtils.getFile("file/end.txt");
        List<String> endLines = FileUtils.readLines(endFile);
        Map<String, Integer> endMap = new HashMap<>();
        for (String line : endLines) {
            String[] splits = line.split("\t");
            if (Integer.parseInt(splits[1]) > 1000)
                endMap.put(splits[0], Integer.parseInt(splits[1]));
        }

        for (String startKey : startMap.keySet())
            for (String endKey : endMap.keySet()) {
                Cell startCell = new Cell(startKey);
                Cell endCell = new Cell(endKey);
                if (Math.abs(startCell.getTileX() - endCell.getTileX()) > 10 && Math.abs(startCell.getTileY() - endCell.getTileY()) > 10) {
                    Set<String> ids = TrajectoryUtil.getAllTrajectoryID(startCell, endCell,"SH");
                    if (ids.size() < 150&&ids.size() > 100 )
                        System.out.println(startCell + " " + endCell + " " + ids.size());
                }

            }
    }
}
