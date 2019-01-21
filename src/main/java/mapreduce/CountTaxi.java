package mapreduce;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * build inverted index by the trajectory set.
 *
 * @author Bin Cheng
 */
public class CountTaxi {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Please input the CITY (SH / SZ) and OutputPath!");
            return;
        } else if (!CommonUtil.isValidCity(args[0]))
            return;

        Configuration config = HBaseConfiguration.create();
        config.setLong("mapred.task.timeout", 60 * 60 * 100);
        Job job = Job.getInstance(config, "CountTaxi");
        job.setJarByClass(CountTaxi.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        String city = args[0];
        String trajectoryTable = CommonUtil.getTrajectoryTable(city);

        TableMapReduceUtil.initTableMapperJob(
                trajectoryTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                Map.class,   // mapper class
                Text.class,             // mapper output key
                IntWritable.class,             // mapper output value
                job);

        job.setReducerClass(Reduce.class);


        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }

    /**
     * put the inverted index into a HBase table
     * that the row key is the tile point,
     * the column is the trajectoryID and
     * the value is the index of title in the trajectory above.
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

        private static int total = 0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            total++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            context.write(new Text("Sum"), new Text(total + ""));
        }
    }

    /**
     * split the whole trajectory into piece of tiles.
     */
    public static class Map extends TableMapper<Text, IntWritable> {
        private static Text mapKey = new Text();
        private static IntWritable mapValue = new IntWritable(1);

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String taxiId = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_INFO.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_ID.getBytes(StandardCharsets.UTF_8)));
            mapKey.set(taxiId);
            context.write(mapKey, mapValue);
        }

    }
}
