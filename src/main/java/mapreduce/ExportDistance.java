package mapreduce;

import bean.GPS;
import constant.HBaseConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import util.CommonUtil;

import java.io.IOException;

/**
 * build inverted index by the trajectory set.
 *
 * @author Bin Cheng
 */
public class ExportDistance {

    /**
     * split the whole trajectory into piece of tiles.
     */
    public static class Map extends TableMapper<Text, DoubleWritable> {

        private Text mapKey = new Text();
        private DoubleWritable mapValue = new DoubleWritable();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            // process data for the row from the Result instance.
            String trajectoryId = Bytes.toString(value.getRow());
//            String taxiId = Bytes.toString(value.getValue(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_ID));
            double distance = Bytes.toDouble(value.getValue(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_DISTANCE));

            mapKey.set(trajectoryId);
            mapValue.set(distance);
            context.write(mapKey, mapValue);

        }

    }

    /**
     * put the inverted index into a HBase table
     * that the row key is the tile point,
     * the column is the trajectoryID and
     * the value is the index of title in the trajectory above.
     */
//    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
//
//        @Override
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//            for (Text val : values) {
//                double distance = computeDistance(val.toString());
//
//                Put put = new Put(key.getBytes());
//                put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_DISTANCE, Bytes.toBytes(distance));
//                context.write(null, put);
//            }
//
//        }
//    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Please outputFilePath!");
            return;
        }

        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "ExportDistance");
        job.setJarByClass(ExportDistance.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                HBaseConstant.TABLE_TRAJECTORY,      // input table
                scan,             // Scan instance to control CF and attribute selection
                Map.class,   // mapper class
                Text.class,             // mapper output key
                DoubleWritable.class,             // mapper output value
                job);
//        TableMapReduceUtil.initTableReducerJob(
//                HBaseConstant.TABLE_TRAJECTORY,        // output table
//                Reduce.class,    // reducer class
//                job);
//
//        job.setNumReduceTasks(36);
        MapFileOutputFormat.setOutputPath(job,new Path(args[0]));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }

//    static double computeDistance(String trajectory) {
//        String[] points = trajectory.substring(1, trajectory.length() - 1).split(", ");
//        if (points.length == 0)
//            return 0;
//
//        double distance = 0.0;
//        GPS lastGPS = new GPS(points[0]);
//
//        for (int i = 1; i < points.length; i++) {
//            GPS currentGPS = new GPS(points[i]);
//            distance += CommonUtil.distanceBetween(lastGPS, currentGPS);
//            lastGPS = currentGPS;
//        }
//
//        return distance;
//    }

}
