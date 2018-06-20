package mapreduce;

import bean.Cell;
import bean.GPS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import constant.CommonConstant;
import constant.HBaseConstant;
import org.apache.commons.lang3.StringUtils;
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
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Sort the timestamp of GPS and generate the GPS trajectory and Cell trajectory,
 * insert them to HBase.
 *
 * @author Bin Cheng
 */
public class SortAndInsert {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;

    @Parameter(names = {"--input", "-i"}, description = "Input filepath.", required = true)
    private String inputFilepath;

    public static void main(String[] args) throws Exception {
        SortAndInsert main = new SortAndInsert();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();
    }


    private void run() throws Exception {

        Configuration config = HBaseConfiguration.create();
        config.set("city", city);
        Job job = Job.getInstance(config, city + " SortAndInsert");

        job.setJarByClass(SortAndInsert.class);
        job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(48);

        TableMapReduceUtil.initTableReducerJob(CommonUtil.getTrajectoryTable(city), Reduce.class, job);
        FileInputFormat.addInputPath(job, new Path(inputFilepath));

        job.waitForCompletion(true);

    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private static String[] splits;
        private static Text outputValue = new Text();
        private static Text taxiId = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String city = context.getConfiguration().get("city");
            switch (city) {
                case CommonConstant.CHENGDU:
                    //出租车ID,纬度,经度,载客状态（1表示载客，0表示无客）,时间点
                    //1, 30.4996330000,103.9771760000,1,2014/08/03 06:01:22
                    splits = value.toString().split(",");
                    taxiId.set(splits[0]);
                    outputValue.set(splits[1] + "," + splits[2] + "," + splits[3] + "," + splits[4]);
                    context.write(taxiId, outputValue);
                    break;
                case CommonConstant.SHANGHAI:
                    //车ID 0,报警,空车 2,顶灯状态,高架mv,刹车,接收时间 6,GPS测定时间 7,经度 8 ,纬度 9,速度,方向,卫星个数
                    //25927,0,0,0,0,0,2015-04-01 20:47:26,2015-04-01 20:47:20,121.535467,31.209523,0.0,201.0,12
                    splits = value.toString().split(",");
                    if (CommonUtil.isValidTimestamp(splits[7])) {
                        taxiId.set(splits[0]);
                        outputValue.set(splits[9] + "," + splits[8] + "," + splits[2] + "," + splits[7]);
                        context.write(taxiId, outputValue);
                    } else {
                        System.err.println(splits[7]);
                    }
                    break;
                case CommonConstant.SHENZHEN:
                    //车ID 0,时间 1,经度 2,纬度 3,速度 4,方向 5,载客 6
                    //B041D7,2009-09-01 09:42:01,114.12487,22.55912,  0, 22,  1, 31
                    splits = value.toString().split(",");
                    if (CommonUtil.isValidTimestamp(splits[1])) {
                        taxiId.set(splits[0]);
                        outputValue.set(splits[3] + "," + splits[2] + "," + StringUtils.trim(splits[6]) + "," + splits[1]);
                        context.write(taxiId, outputValue);
                    } else {
                        System.err.println(splits[1]);
                    }
                    break;
                default:
            }

        }
    }

    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {

        private SimpleDateFormat sdf;
        private List<String[]> list = new ArrayList<>();
        private List<GPS> trajectory = new ArrayList<>();
        private GPS current;
        private GPS last;
        private int numThreshold = 1000;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String city = context.getConfiguration().get("city");
            switch (city) {
                case CommonConstant.CHENGDU:
                    sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    break;
                case CommonConstant.SHANGHAI:
                case CommonConstant.SHENZHEN:
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    break;
                default:
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String city = context.getConfiguration().get("city");
            int numOfZero = 0, numOfOne = 0;
            list.clear();
            for (Text text : values) {
                String[] splits = text.toString().split(",");
                if ("0".equals(splits[2])) {
                    numOfZero++;
                } else {
                    numOfOne++;
                }
                list.add(splits);
            }
            if (numOfZero < numThreshold || numOfOne < numThreshold) {
                return;
            }
            try {
                list.sort((o1, o2) -> {
                    try {
                        Date a = sdf.parse(o1[3]);
                        Date b = sdf.parse(o2[3]);
                        return a.compareTo(b);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        return 0;
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(list);
                return;
            }

            //used to cutting trajectory
            boolean flag = false;

            for (String[] item : list) {
                //taxi is occupied, the GPS point is useful
                boolean occupied = (CommonConstant.SHENZHEN.equals(city) || CommonConstant.CHENGDU.equals(city)) && "1".equals(item[2]) ||
                        CommonConstant.SHANGHAI.equals(city) && "0".equals(item[2]);
                if (occupied) {
                    try {
                        current = new GPS(Double.parseDouble(item[0]), Double.parseDouble(item[1]), sdf.parse(item[3]));
                        if (trajectory.size() != 0) {
                            last = trajectory.get(trajectory.size() - 1);
                            //unit:s
                            int interval = (int) (current.getTimestamp().getTime() - last.getTimestamp().getTime()) / 1000;
                            //unit:m
                            double distance = CommonUtil.distanceBetween(current, last);
                            //unit:m/s
                            double speed = distance / interval;

                            //limit:120km/h
                            if (speed <= 33.333) {
                                if (distance > 5000 || interval > 10 * 60) {
                                    Put put = preProcessTrajectory(key.toString(), trajectory);
                                    if (put != null) {
                                        context.write(null, put);
                                    }
                                    trajectory.clear();
                                } else {
                                    trajectory.add(current);
                                }
                            }
                        } else {
                            trajectory.add(current);
                        }
                        flag = true;
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                } else if (flag) {
                    Put put = preProcessTrajectory(key.toString(), trajectory);
                    if (put != null) {
                        context.write(null, put);
                    }
                    trajectory.clear();
                    flag = false;
                }
            }
        }
    }

    private static final double DISTANCE_LIMIT = 65 * 1000;
    private static final int CELL_LOWER_BOUND = 5;

    /**
     * Map the GPS trajectory to Cell trajectory, and convert the form of data so that it can be inserted into HBase.
     */
    private static Put preProcessTrajectory(String taxiID, List<GPS> trajectory) throws UnsupportedEncodingException {

        //ignore while the trajectory is too long.
        double distance = util.CommonUtil.trajectoryDistance(trajectory);
        if (distance > DISTANCE_LIMIT) {
            return null;
        }

        //generate grid cell trajectory.
        List<Cell> cells = GridUtil.gridGPSSequence(trajectory);

        //if the number of cells are less than 5, we suppose that this trajectory is noise data.
        if (cells.size() < CELL_LOWER_BOUND) {
            return null;
        }

        Put put = new Put(Bytes.toBytes(CommonUtil.getUUID()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO.getBytes("UTF-8"), HBaseConstant.COLUMN_ID.getBytes("UTF-8"), Bytes.toBytes(taxiID));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes("UTF-8"), HBaseConstant.COLUMN_CELL.getBytes("UTF-8"), Bytes.toBytes(cells.toString()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_TRAJECTORY.getBytes("UTF-8"), HBaseConstant.COLUMN_GPS.getBytes("UTF-8"), Bytes.toBytes(trajectory.toString()));
        put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO.getBytes("UTF-8"), HBaseConstant.COLUMN_DISTANCE.getBytes("UTF-8"), Bytes.toBytes(distance));
        return put;

    }
}
