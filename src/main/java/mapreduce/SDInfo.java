package mapreduce;

import bean.Cell;
import bean.GPS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import constant.HBaseConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.CommonUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Count the hotspot of source and destination.
 *
 * @author Bin Cheng
 */
public class SDInfo {


    @Parameter(names = {"--output", "-o"}, description = "The output path.", required = true)
    private String outputPath;
    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;

    public static void main(String[] args) throws Exception {
        SDInfo main = new SDInfo();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();
    }

    private void run() throws IOException, ClassNotFoundException, InterruptedException {
        Path result = new Path(outputPath);
        Configuration config = HBaseConfiguration.create();

        //job
        Job job = Job.getInstance(config);
        job.setJarByClass(SDInfo.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                CommonUtil.getTrajectoryTable(city),      // input table
                scan,             // Scan instance to control CF and attribute selection
                Mapper.class,   // mapper class
                Text.class,             // mapper output key
                Text.class,             // mapper output value
                job);
        job.setReducerClass(Reduce.class);
        FileOutputFormat.setOutputPath(job, result);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);

    }

    public static class Mapper extends TableMapper<Text, Text> {

        private static Text mapKey = new Text(), mapValue = new Text();
        private Cell startCell, endCell;
        private GPS startGPS, endGPS;

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {

            String trajectoryCell = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_CELL.getBytes(StandardCharsets.UTF_8)));
            String trajectoryGPS = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_GPS.getBytes(StandardCharsets.UTF_8)));

            String[] tiles = trajectoryCell.substring(1, trajectoryCell.length() - 1).split(", ");
            String[] gpss = trajectoryGPS.substring(1, trajectoryGPS.length() - 1).split(", ");

            startCell = new Cell(tiles[0]);
            endCell = new Cell(tiles[tiles.length - 1]);
            startGPS = new GPS(gpss[0]);
            endGPS = new GPS(gpss[gpss.length - 1]);

            mapKey.set(new Text(startCell.toString() + " " + endCell.toString()));
            mapValue.set(startGPS.getTimestamp().getHours() + " " + CommonUtil.timeBetween(startGPS, endGPS));
            context.write(mapKey, mapValue);

        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Map<Integer, List<Double>> map = new HashMap<>();
            for (Text val : values) {
                String[] splits = val.toString().split(" ");
                Integer hour = Integer.valueOf(splits[0]);
                Double time = Double.valueOf(splits[1]);
                if (!map.containsKey(hour)) {
                    count++;
                    map.put(hour, new ArrayList<>());
                }
                map.get(hour).add(time);
            }
            if (count == 24) {
                context.write(key, new Text(new Gson().toJson(map)));
            }
        }
    }
}
