package test;

import bean.Cell;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.CommonUtil;
import util.TileSystem;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Test
 *
 * @author Bin Cheng
 */
public class Test {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;
    @Parameter(names = {"--output", "-o"}, description = "Output File Path.", required = true)
    private String output;

    public static void main(String[] args) throws Exception {
        Test main = new Test();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();
    }

    private void run() throws Exception {

        Configuration config = HBaseConfiguration.create();
        config.setLong("mapred.task.timeout", 60 * 60 * 100);
        Job job = Job.getInstance(config, "Test");
        job.setJarByClass(Test.class);

        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);

//        String trajectoryTable = CommonUtil.getTrajectoryTable(city);
        String invertedTable = CommonUtil.getInvertedTable(city);

        TableMapReduceUtil.initTableMapperJob(invertedTable, scan, Map.class, Text.class, IntWritable.class, job);
//        TableMapReduceUtil.initTableReducerJob(invertedTable, mapreduce.CreateInvertIndex.Reduce.class, job);
        job.setReducerClass(Reduce.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
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


    public static class Map extends TableMapper<Text, IntWritable> {

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String gridId = Bytes.toString(value.getRow());
            int num = value.getFamilyMap(HBaseConstant.COLUMN_FAMILY_INDEX.getBytes(StandardCharsets.UTF_8)).size();
            context.write(new Text(gridId + "\t" + TileSystem.TileToGPS(new Cell(gridId))), new IntWritable(num));
        }

    }
}
