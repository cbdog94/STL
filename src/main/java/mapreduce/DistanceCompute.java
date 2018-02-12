package mapreduce;

import bean.GPS;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
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
import util.CommonUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * build inverted index by the trajectory set.
 *
 * @author Bin Cheng
 */
public class DistanceCompute {

    /**
     * split the whole trajectory into piece of tiles.
     */
    public static class Map extends TableMapper<Text, Text> {

        private Text mapKey = new Text();
        private Text mapValue = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            // process data for the row from the Result instance.
            String trajectoryId = Bytes.toString(value.getRow());
//            String taxiId = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_ID));
            String trajectory = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_TRAJECTORY, HBaseConstant.COLUMN_GPS));

            mapKey.set(trajectoryId);
            mapValue.set(trajectory);
            context.write(mapKey, mapValue);

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
                double distance = computeDistance(val.toString());

                Put put = new Put(key.getBytes());
                put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_DISTANCE, Bytes.toBytes(distance));
                context.write(null, put);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "DistanceCompute");
        job.setJarByClass(DistanceCompute.class);    // class that contains mapper

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
                HBaseConstant.TABLE_TRAJECTORY,        // output table
                Reduce.class,    // reducer class
                job);

        job.setNumReduceTasks(36);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }

    static double computeDistance(String trajectory) {
        String[] points = trajectory.substring(1, trajectory.length() - 1).split(", ");
        if (points.length == 0)
            return 0;

        double distance = 0.0;
        GPS lastGPS = new GPS(points[0]);

        for (int i = 1; i < points.length; i++) {
            GPS currentGPS = new GPS(points[i]);
            distance += CommonUtil.distanceBetween(lastGPS, currentGPS);
            lastGPS = currentGPS;
        }

        return distance;
    }

}
