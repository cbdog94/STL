package mapreduce;

import bean.Cell;
import bean.GPS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import constant.HBaseConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import util.CommonUtil;
import util.GridUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sort the timestamp of GPS and generate the GPS trajectory and Cell trajectory,
 * insert them to HBase.
 *
 * @author Bin Cheng
 */
public class SortAndInsert_XA {

    private static final double DISTANCE_LIMIT_MAX = 65 * 1000;
    private static final double DISTANCE_LIMIT_MIN = 1000;
    private static final int CELL_LOWER_BOUND = 5;
    //    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
//    private String city;
    @Parameter(names = {"--input", "-i"}, description = "Input filepath.", required = true)
    private String inputFilepath;

    public static void main(String[] args) throws Exception {
        SortAndInsert_XA main = new SortAndInsert_XA();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();
    }

    /**
     * Map the GPS trajectory to Cell trajectory, and convert the form of data so that it can be inserted into HBase.
     */
    private static Put preProcessTrajectory(String orderID, List<GPS> trajectory) {

        //ignore while the trajectory is too long.
        double distance = CommonUtil.trajectoryDistance(trajectory);
        if (distance > DISTANCE_LIMIT_MAX || distance < DISTANCE_LIMIT_MIN) {
            return null;
        }

        //generate grid cell trajectory.
        List<Cell> cells = GridUtil.gridGPSSequence(trajectory);

        //if the number of cells are less than 5, we suppose that this trajectory is noise data.
        if (cells.size() < CELL_LOWER_BOUND) {
            return null;
        }

        Put put = new Put(Bytes.toBytes(CommonUtil.getUUID()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_ID.getBytes(StandardCharsets.UTF_8), Bytes.toBytes(orderID));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_CELL.getBytes(StandardCharsets.UTF_8), Bytes.toBytes(cells.toString()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_GPS.getBytes(StandardCharsets.UTF_8), Bytes.toBytes(trajectory.toString()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO.getBytes(StandardCharsets.UTF_8), HBaseConstant.COLUMN_DISTANCE.getBytes(StandardCharsets.UTF_8), Bytes.toBytes(distance));
        return put;

    }

    private void run() throws Exception {

        Configuration config = HBaseConfiguration.create();

        Job job = Job.getInstance(config, "XA SortAndInsert");

        job.setJarByClass(SortAndInsert_XA.class);
        job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(48);

        TableMapReduceUtil.initTableReducerJob(CommonUtil.getTrajectoryTable("XA"), Reduce.class, job);
        FileInputFormat.addInputPath(job, new Path(inputFilepath));

        job.waitForCompletion(true);

    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private static String[] splits;
        private static Text outputValue = new Text();
        private static Text orderId = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //出租车ID,订单ID,时间,经度,纬度
            //3ca44419968b58109b4858afa2bfb03d,9cebe37c4b564063f81c5759ff4fc101,1476009294,108.94065,34.23004
            splits = value.toString().split(",");
            orderId.set(splits[1]);
            outputValue.set(splits[2] + "," + splits[3] + "," + splits[4]);
            context.write(orderId, outputValue);
        }
    }

    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {


        private List<String[]> list = new ArrayList<>();
        private List<GPS> trajectory = new ArrayList<>();
        private int numThreshold = 20;


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            list.clear();
            for (Text text : values) {
                String[] splits = text.toString().split(",");
                list.add(splits);
            }
            if (list.size() < numThreshold) {
                return;
            }
            list.sort(Comparator.comparing(o -> Long.valueOf(o[0])));
            trajectory = list.stream().map(x -> new GPS(Double.valueOf(x[2]), Double.valueOf(x[1]), new Date(Long.parseLong(x[0]) * 1000))).collect(Collectors.toList());
            Put put = preProcessTrajectory(key.toString(), trajectory);
            if (put != null) {
                context.write(null, put);
            }
        }
    }
}