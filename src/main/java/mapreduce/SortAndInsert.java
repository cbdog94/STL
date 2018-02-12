package mapreduce;

import bean.Cell;
import bean.GPS;
import com.graphhopper.GraphHopper;
import com.graphhopper.matching.EdgeMatch;
import com.graphhopper.matching.GPXExtension;
import com.graphhopper.matching.MapMatching;
import com.graphhopper.matching.MatchResult;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.util.CarFlagEncoder;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.util.GPXEntry;
import com.graphhopper.util.Parameters;
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
public class SortAndInsert {


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private static String[] splits;
        private static Text outputValue = new Text();
        private static Text taxiId = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
        }
    }

    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
        //notice:HH means 24-hour clock, hh means 12-hour clock
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private List<String[]> list = new ArrayList<>();
        private List<GPS> trajectory = new ArrayList<>();
        private GPS current;
        private GPS last;

//        private static MapMatching mapMatching;
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
//            // import OpenStreetMap data
//            GraphHopper hopper = new GraphHopperOSM();
////            hopper.setDataReaderFile("/home/hadoop/openStreetMap/shanghai_china.osm.pbf");
//            hopper.setGraphHopperLocation("/home/hadoop/openStreetMap/mapmatching");
//            CarFlagEncoder encoder = new CarFlagEncoder();
//            hopper.setEncodingManager(new EncodingManager(encoder));
//            hopper.getCHFactoryDecorator().setEnabled(false);
//            hopper.importOrLoad();
//
//            // create MapMatching object, can and should be shared accross threads
//            String algorithm = Parameters.Algorithms.DIJKSTRA_BI;
//            Weighting weighting = new FastestWeighting(encoder);
//            AlgorithmOptions algoOptions = new AlgorithmOptions(algorithm, weighting);
//            mapMatching = new MapMatching(hopper, algoOptions);
//            mapMatching.setMeasurementErrorSigma(35.0);
//        }

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

                //taxi is busy, the GPS point is useful
                //TODO
                if ("0".equals(item[2])) {
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

                                    preProcessTrajectory(key.toString(), trajectory, context);

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
                    preProcessTrajectory(key.toString(), trajectory, context);
                    trajectory.clear();
                    flag = false;
                }
            }
        }

        private final double DISTANCE_LIMIT = 65 * 1000;

        /**
         * pre-processing
         */
        private void preProcessTrajectory(String taxiID, List<GPS> trajectory, Context context) throws IOException, InterruptedException {
//            try {
//                trajectory = doMapMatching(trajectory);
//            } catch (Exception e) {
//                return;
//            }

            //距离太长的去掉
            double distance = CommonUtil.trajectoryDistance(trajectory);
            if (distance > DISTANCE_LIMIT)
                return;

            //生成网格轨迹
            List<Cell> cells = GridUtil.gridGPSSequence(trajectory);

            //if the number of cells are less than 5, we suppose that this trajectory is noise data.
            if (cells.size() < 5)
                return;

            Put put = new Put(Bytes.toBytes(CommonUtil.getUUID()));
            put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_ID, Bytes.toBytes(taxiID));
            put.addColumn(HBaseConstant.COLUMN_FAMILY_TRAJECTORY, HBaseConstant.COLUMN_TRAJECTORY, Bytes.toBytes(cells.toString()));
            put.addColumn(HBaseConstant.COLUMN_FAMILY_TRAJECTORY, HBaseConstant.COLUMN_GPS, Bytes.toBytes(trajectory.toString()));
            put.addColumn(HBaseConstant.COLUMN_FAMILY_INFO, HBaseConstant.COLUMN_DISTANCE, Bytes.toBytes(distance));
            context.write(null, put);

        }

//        private List<GPS> doMapMatching(List<GPS> trajectory) {
//
//            List<GPXEntry> GPXEntries = new ArrayList<>();
//            for (GPS point : trajectory) {
//                GPXEntries.add(new GPXEntry(point.getLatitude(), point.getLongitude(), point.getTimestamp().getTime()));
//            }
//            MatchResult mr = mapMatching.doWork(GPXEntries);
//
//            List<GPS> GPSPoints = new ArrayList<>();
//
//            for (EdgeMatch edge : mr.getEdgeMatches()) {
//                for (GPXExtension gpXExtension : edge.getGpxExtensions()) {
//                    GPXEntry gpxEntry = gpXExtension.getEntry();
//                    GPSPoints.add(new GPS(gpxEntry.getLat(), gpxEntry.getLon(), new Date(gpxEntry.getTime())));
//                }
//            }
//
//            return GPSPoints;
//        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Please input the filePath!");
            return;
        }
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "SortAndInsert");

        job.setJarByClass(SortAndInsert.class);
        job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(36);

        TableMapReduceUtil.initTableReducerJob(
                HBaseConstant.TABLE_TRAJECTORY,      // output table
                Reduce.class,             // reducer class
                job);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.waitForCompletion(true);

    }
}
