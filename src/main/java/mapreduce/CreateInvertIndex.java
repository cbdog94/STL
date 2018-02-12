package mapreduce;

import constant.HBaseConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * build inverted index by the trajectory set.
 *
 * @author Bin Cheng
 */
public class CreateInvertIndex {

    /**
     * split the whole trajectory into piece of tiles.
     */
    public static class Map extends TableMapper<Text, Text> {

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            // process data for the row from the Result instance.
            String trajectoryId = Bytes.toString(value.getRow());
            String taxiId = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_ID));
            String trajectory = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY, HBaseConstant.COLUMN_TRAJECTORY));

            String[] tiles = trajectory.substring(1, trajectory.length() - 1).split(", ");
            for (int i = 0; i < tiles.length; i++) {
                context.write(new Text(tiles[i]), new Text(trajectoryId + "," + i));
            }
        }

    }

    /**
     * put the inverted index into a HBase table
     * that the row key is the tile point,
     * the column is the trajectoryID and
     * the value is the index of title in the trajectory above.
     */
    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                //value: 0006d72c38bb4c7ab035f6ebd8de6ad6,1
                //so the splits[0] is trajectoryID and the splits[1] is the index where one certain tile located in this trajectory
                String[] splits = val.toString().split(",");
                Put put = new Put(key.getBytes());
                put.addColumn(HBaseConstant.COLUMN_FAMILY_INDEX, splits[0].getBytes(), splits[1].getBytes());
                context.write(null, put);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.setLong("mapred.task.timeout", 60 * 60 * 100);
        Job job = Job.getInstance(config, "CreateInvertIndex");
        job.setJarByClass(CreateInvertIndex.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                HBaseConstant.TABLE_TRAJECTORY,      // input table
                scan,             // Scan instance to control CF and attribute selection
                Map.class,   // mapper class
                Text.class,             // mapper output key
                Text.class,             // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                HBaseConstant.TABLE_TRAJECTORY_INVERTED,        // output table
                Reduce.class,    // reducer class
                job);

        job.setNumReduceTasks(36);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
