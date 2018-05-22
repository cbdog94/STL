import bean.Cell;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import hbase.TrajectoryUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import util.CommonUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

public class FindPair {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;

    private static String hdfs = "hdfs://192.168.0.53:9000";

    public static void main(String... argv) {
        FindPair main = new FindPair();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        try {
            main.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void run() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", hdfs);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(new URI(hdfs), conf);

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfs + "/" + city.toLowerCase() + "_start_hotspot/part-r-00000"))));
        List<String> SHStart = br.lines().map(s -> s.split("\t")[0]).collect(Collectors.toList());
        List<String> SHSUBStart = SHStart.subList(SHStart.size() - 10, SHStart.size());

        br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfs + "/" + city.toLowerCase() + "_end_hotspot/part-r-00000"))));
        List<String> SHEnd = br.lines().map(s -> s.split("\t")[0]).collect(Collectors.toList());
        List<String> SHSubEnd = SHEnd.subList(SHEnd.size() - 10, SHEnd.size());


        SHSUBStart.parallelStream().forEach(start -> {
            SHSubEnd.parallelStream().forEach(end -> {
                Cell startCell = new Cell(start), endCell = new Cell(end);
                if (Math.pow(endCell.getTileY() - startCell.getTileY(), 2) + Math.pow(endCell.getTileX() - startCell.getTileX(), 2) > 100) {
                    int size = TrajectoryUtil.getAllTrajectoryGPSs(startCell, endCell, city).size();
                    if (size > 100)
                        System.out.println(start + " " + end + " " + size);
                }
            });
        });

    }
}
