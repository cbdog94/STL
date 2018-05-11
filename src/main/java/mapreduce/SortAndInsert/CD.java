package mapreduce.SortAndInsert;

import bean.GPS;
import constant.HBaseConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import util.CommonUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Sort the timestamp of GPS and generate the GPS trajectory and Cell trajectory,
 * insert them to HBase.
 * <p>
 * For Chengdu Data set.
 *
 * @author Bin Cheng
 */
public class CD {


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Please input the FILEPATH !");
            return;
        }
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "CD SortAndInsert");

        job.setJarByClass(CD.class);
        job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(48);

        TableMapReduceUtil.initTableReducerJob(
                HBaseConstant.TABLE_CD_TRAJECTORY,      // output table
                Reduce.class,             // reducer class
                job);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.waitForCompletion(true);

    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private static String[] splits;
        private static Text outputValue = new Text();
        private static Text taxiId = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //出租车ID,纬度,经度,载客状态（1表示载客，0表示无客）,时间点
            //1, 30.4996330000,103.9771760000,1,2014/08/03 06:01:22
            splits = value.toString().split(",");
            taxiId.set(splits[0]);
            outputValue.set(splits[1] + "," + splits[2] + "," + splits[3] + "," + splits[4]);
            context.write(taxiId, outputValue);
        }
    }

    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
        //notice:HH means 24-hour clock, hh means 12-hour clock
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        private List<String[]> list = new ArrayList<>();
        private List<GPS> trajectory = new ArrayList<>();
        private GPS current;
        private GPS last;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //sample: 30.641017,104.095475,0,2014-08-03 15:23:45
            int numOfZero = 0;
            int numOfOne = 0;

            list.clear();
            for (Text text : values) {
                String[] splits = text.toString().split(",");
                if ("0".equals(splits[2]))
                    numOfZero++;
                else
                    numOfOne++;
                list.add(splits);
            }
            if (numOfZero < 1000 || numOfOne < 1000)
                return;

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

            boolean flag = false;//used to cutting trajectory

            for (String[] item : list) {

                //taxi is occupied, the GPS point is useful
                if ("1".equals(item[2])) {
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
                                    Put put = CommonUtil.preProcessTrajectory(key.toString(), trajectory);
                                    if (put != null)
                                        context.write(null, put);
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
                    Put put = CommonUtil.preProcessTrajectory(key.toString(), trajectory);
                    if (put != null)
                        context.write(null, put);
                    trajectory.clear();
                    flag = false;
                }
            }
        }

    }
}
