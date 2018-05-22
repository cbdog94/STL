package mapreduce;

import bean.Cell;
import bean.GPS;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import util.CommonUtil;
import util.TileSystem;

import java.io.IOException;

/**
 * Count the hotspot of source and destination.
 *
 * @author Bin Cheng
 */
public class HotSpot {


    public static class Map extends TableMapper<Text, IntWritable> {

        private static IntWritable one = new IntWritable(1);
        private static Text mapKey = new Text();
        private Cell cell;

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {

            String trajectory = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY, HBaseConstant.COLUMN_CELL));

            String[] tiles = trajectory.substring(1, trajectory.length() - 1).split(", ");

            if (context.getConfiguration().getBoolean("start", true)) {
                cell = new Cell(tiles[0]);
            } else {
                cell = new Cell(tiles[tiles.length - 1]);
            }

            mapKey.set(new Text("[" + cell.getTileX() + "," + cell.getTileY() + "]"));

            context.write(mapKey, one);

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

    public static class ReduceSort extends Reducer<IntWritable, Text, Text, IntWritable> {
        private static GPS gps;
        private static Text outputKey = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                gps = TileSystem.TileToGPS(new Cell(val.toString()));
                outputKey.set(val + "\t" + gps);
                context.write(outputKey, key);
            }
        }
    }

    @Parameter(names = {"--output", "-o"}, description = "The output path.", required = true)
    private String outputPath;
    @Parameter(names = {"--start", "-s"}, description = "If True, count the hotspot of start, otherwise count the hotspot of end.")
    private boolean start = false;
    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;

    public static void main(String[] args) throws Exception {
        HotSpot main = new HotSpot();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();
    }

    private void run() throws IOException, ClassNotFoundException, InterruptedException {
        Path tempPath = new Path("/hotspot_tmp");
        Path result = new Path(outputPath);
        Configuration config = HBaseConfiguration.create();
        config.setBoolean("start", start);

        //job
        Job job = Job.getInstance(config, city + (start ? " Start" : " End") + " HotSpot");
        job.setJarByClass(HotSpot.class);    // class that contains mapper

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
        Job jobSort = Job.getInstance(config, city + (start ? "_Start" : "_End") + "_HotSpot_Sort");
        FileInputFormat.addInputPath(jobSort, tempPath);
        jobSort.setInputFormatClass(SequenceFileInputFormat.class);
        jobSort.setJarByClass(HotSpot.class);
        jobSort.setMapperClass(InverseMapper.class);
        jobSort.setMapOutputKeyClass(IntWritable.class);
        jobSort.setMapOutputValueClass(Text.class);
        jobSort.setReducerClass(ReduceSort.class);
        jobSort.setOutputKeyClass(Text.class);
        jobSort.setOutputValueClass(IntWritable.class);
//        FileSystem.get(config).delete(result, true);
        FileOutputFormat.setOutputPath(jobSort, result);
        jobSort.waitForCompletion(true);

        FileSystem.get(config).delete(tempPath, true);
    }
}
