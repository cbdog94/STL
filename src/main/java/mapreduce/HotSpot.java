package mapreduce;

import bean.Cell;
import bean.GPS;
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
import util.TileSystem;

import java.io.IOException;

/**
 * build inverted index by the trajectory set.
 *
 * @author Bin Cheng
 */
public class HotSpot {

    /**
     * split the whole trajectory into piece of tiles.
     */
    public static class Map extends TableMapper<Text, IntWritable> {

        private static IntWritable one = new IntWritable(1);
        private static Text mapKey = new Text();
        private Cell cell;

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {

            String trajectory = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY, HBaseConstant.COLUMN_TRAJECTORY));

            String[] tiles = trajectory.substring(1, trajectory.length() - 1).split(", ");

            if (context.getConfiguration().get("type").equals("s")) {
                cell = new Cell(tiles[0]);
            } else {
                cell = new Cell(tiles[tiles.length - 1]);
            }

            mapKey.set(new Text("[" + cell.getTileX() + "," + cell.getTileY() + "]"));

            context.write(mapKey, one);

        }

    }

    /**
     * put the inverted index into a HBase table
     * that the row key is the tile point,
     * the column is the trajectoryID and
     * the value is the index of title in the trajectory above.
     */
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


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Please input outputPath");
            return;
        } else if (args.length < 2 || (!args[1].equals("s") && !args[1].equals("e"))) {
            System.out.println("Please s(tart) or e(nd)");
            return;
        }

        Configuration config = HBaseConfiguration.create();
        config.set("type", args[1]);

        Job job = Job.getInstance(config, "HotSpotStart");
        job.setJarByClass(HotSpot.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                HBaseConstant.TABLE_TRAJECTORY,      // input table
                scan,             // Scan instance to control CF and attribute selection
                Map.class,   // mapper class
                Text.class,             // mapper output key
                IntWritable.class,             // mapper output value
                job);

        job.setReducerClass(Reduce.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
