package test;

import bean.Cell;
import bean.GPS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import hbase.TrajectoryUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.CommonUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test
 *
 * @author Bin Cheng
 */
public class Test2 {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;
    @Parameter(names = {"--output", "-o"}, description = "Output File Path.", required = true)
    private String output;
    @Parameter(names = {"--input", "-i"}, description = "Input File Path.", required = true)
    private String input;

    public static void main(String[] args) throws Exception {
        Test2 main = new Test2();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);
        main.run();
    }

    private void run() throws Exception {

        Configuration config = HBaseConfiguration.create();
        config.set("city", city);
        config.setInt(MRJobConfig.NUM_MAPS, 50);
        config.setLong("mapreduce.input.fileinputformat.split.maxsize", 10 * 1024 * 1024);
        config.setLong("mapred.task.timeout", 60 * 60 * 100);
        Job job = Job.getInstance(config, "Test2");
        job.setJarByClass(Test2.class);

        job.setMapperClass(TestMapper.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }


    public static class TestMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static String[] splits;
        private static Cell start, end;
        private static Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String city = context.getConfiguration().get("city");
            splits = value.toString().split("\t");
            start = new Cell(splits[0]);
            end = new Cell(splits[1]);
            if (start.equals(end)) {
                return;
            }
            Map<String, List<GPS>> all = TrajectoryUtil.getAllTrajectoryGPSs(start, end, city);
            Map<Integer, List<Double>> result = new HashMap<>();
            for (List<GPS> trajectory : all.values()) {
                int hour = trajectory.get(0).getTimestamp().getHours();
                double time = CommonUtil.timeBetween(trajectory.get(0), trajectory.get(trajectory.size() - 1));
                if (!result.containsKey(hour)) {
                    result.put(hour, new ArrayList<>());
                }
                result.get(hour).add(time);
            }
            if (result.size() != 24) {
                return;
            }
            outputValue.set(new Gson().toJson(result));
            context.write(value, outputValue);
        }
    }
}
