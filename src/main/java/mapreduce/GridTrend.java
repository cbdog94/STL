package mapreduce;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import constant.HBaseConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import util.CommonUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Count the hotspot of source and destination.
 *
 * @author Bin Cheng
 */
public class GridTrend {


    @Parameter(names = {"--output", "-o"}, description = "The output path.", required = true)
    private String outputPath;
    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;

    public static void main(String[] args) throws Exception {
        GridTrend main = new GridTrend();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();
    }

    private void run() throws IOException, ClassNotFoundException, InterruptedException {
        Path tempPath = new Path("/gridTrend_tmp");
        Path result = new Path(outputPath);
        Configuration config = HBaseConfiguration.create();

        //job
        Job job = Job.getInstance(config, city + "_GridTrend_1");
        job.setJarByClass(GridTrend.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                CommonUtil.getTrajectoryTable(city),      // input table
                scan,             // Scan instance to control CF and attribute selection
                Map.class,   // mapper class
                Text.class,             // mapper output key
                IntWritable.class,             // mapper output value
                job);
        job.setReducerClass(Reduce.class);
        FileOutputFormat.setOutputPath(job, tempPath);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(36);
        job.waitForCompletion(true);

        //job sort
        Job job2 = Job.getInstance(config, city + "_GridTrend_2");
        FileInputFormat.addInputPath(job2, tempPath);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setJarByClass(GridTrend.class);
        job2.setMapperClass(Map2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(Reduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, result);
        job2.waitForCompletion(true);

        FileSystem.get(config).delete(tempPath, true);
    }

    public static class Map extends TableMapper<Text, IntWritable> {

        private static IntWritable one = new IntWritable(1);
        private static Text mapKey = new Text();
        private static Pattern pattern = Pattern.compile(".*\\s(\\d+):.*");

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {

            String cellTrajectory = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_CELL.getBytes(StandardCharsets.UTF_8)));
            String GPSTrajectory = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_GPS.getBytes(StandardCharsets.UTF_8)));

            String[] tiles = cellTrajectory.substring(1, cellTrajectory.length() - 1).split(", ");
            String[] GPSs = GPSTrajectory.substring(1, GPSTrajectory.length() - 1).split(", ");

            int startHour = getHour(GPSs[0]), endHour = getHour(GPSs[GPSs.length - 1]);
            int period = (endHour + 24 - startHour) % 24 + 1, part = tiles.length / period;

            for (int i = 0; i < period - 1; i++) {
                for (int j = i * part; j < (i + 1) * part; j++) {
                    mapKey.set(new Text(tiles[j] + " " + (startHour + i) % 24));
                    context.write(mapKey, one);
                }
            }
            for (int j = (period - 1) * part; j < tiles.length; j++) {
                mapKey.set(new Text(tiles[j] + " " + (startHour + period - 1) % 24));
                context.write(mapKey, one);
            }
        }

        private int getHour(String GPS) {
            Matcher matcher = pattern.matcher(GPS);
            if (matcher.find()) {
                return Integer.parseInt(matcher.group(1));
            }
            return -1;
        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Map2 extends Mapper<Text, IntWritable, Text, Text> {

        private static Text mapValue = new Text();

        @Override
        public void map(Text key, IntWritable value, Context context) throws InterruptedException, IOException {
            String[] splits = key.toString().split(" ");
            mapValue.set(splits[1] + " " + value);
            context.write(new Text(splits[0]), mapValue);
        }

    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] count = new int[24];
            Arrays.fill(count, -1);
            for (Text val : values) {
                String[] splits = val.toString().split(" ");
                count[Integer.parseInt(splits[0])] = Integer.parseInt(splits[1]);
            }
            context.write(key, new Text(Arrays.toString(count)));
        }
    }
}
