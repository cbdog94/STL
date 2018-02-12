package mapreduce;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.CommonUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Sort by the timestamp of GPS and the taxi,
 * and then grid the GPS to tile point,
 * insert the tile sequence to HBase.
 *
 * @author Bin Cheng
 */
public class DailyTrajectory {


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //车ID 0,报警,空车 2,顶灯状态,高架mv,刹车,接收时间 6,GPS测定时间 7,经度 8 ,纬度 9,速度,方向,卫星个数
            //25927,0,0,0,0,0,2015-04-01 20:47:26,2015-04-01 20:47:20,121.535467,31.209523,0.0,201.0,12
            String[] splits = line.split(",");
            if (CommonUtil.isValidTimestamp(splits[7])) {
                if (splits[0].equals("30040"))
                    context.write(new Text(splits[0]), new Text(splits[9] + "," + splits[8] + "," + splits[2] + "," + splits[7] + "," + splits[10] + "," + splits[11]));
            } else {
                System.err.println(splits[7]);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        //notice:HH means 24-hour clock, hh means 12-hour clock
        private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //sample: 30.641017,104.095475,0,2014-08-03 15:23:45
            List<PointInfo> list = new ArrayList<>();
            for (Text text : values) {
                list.add(new PointInfo(text.toString()));
            }

            try {
                list.sort((o1, o2) -> {

                    if (o1.timestamp.before(o2.timestamp)) {
                        return -1;
                    } else if (o1.timestamp.after(o2.timestamp)) {
                        return 1;
                    } else {
                        return 0;
                    }

                });
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(list);
                return;
            }

            context.write(null, new Text(new Gson().toJson(new Trajectory(key.toString(), list))));

        }
    }

    public static class Trajectory {
        public String taxiId;
        public List<PointInfo> points;

        Trajectory(String taxiId, List<PointInfo> points) {
            this.taxiId = taxiId;
            this.points = points;
        }
    }

    public static class PointInfo {
        public double lat;
        public double lng;
        public int status;
        public Date timestamp;
        public double speed;
        public double direct;

        //sample: 30.641017,104.095475,0,2014-08-03 15:23:45
        public PointInfo(String pointInfo) {
            String[] splits = pointInfo.split(",");
            this.lat = Double.parseDouble(splits[0]);
            this.lng = Double.parseDouble(splits[1]);
            this.status = Integer.parseInt(splits[2]);
            try {
                this.timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(splits[3]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            this.speed = Double.parseDouble(splits[4]);
            this.direct = Double.parseDouble(splits[5]);
        }

        public PointInfo(){}
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Please input inputFilePath and outputFilePath!");
            return;
        }

        Job job = Job.getInstance(new Configuration(), "DailyTrajectory");

        job.setJarByClass(DailyTrajectory.class);
        job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reduce.class);

//        job.setNumReduceTasks(12);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
