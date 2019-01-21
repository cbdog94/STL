package app;

import bean.Cell;
import bean.GPS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.StatUtils;
import util.CommonUtil;
import util.TileSystem;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Spatial-Temporal Laws
 */
public class Test {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", validateWith = CommonUtil.CityValidator.class)
    private String city = "SH";
    @Parameter(names = {"--debug", "-d"}, description = "Debug mode.")
    private Boolean debug = false;
    //    @Parameter(names = {"--degree", "-deg"}, description = "Model complexity.")
//    private int degree = 3;
    @Parameter(names = {"-t"}, description = "Threshold of size.")
    private double threshold = 0.8;
//    @Parameter(names = {"-tD"}, description = "Threshold of distance.")
//    private double thresholdDist = 0.7;

    @Parameter(names = {"-s"}, description = "Start cell.", validateWith = CommonUtil.CellValidator.class)
    private String startCell = "[109776,53554]";
    @Parameter(names = {"-e"}, description = "End cell.", validateWith = CommonUtil.CellValidator.class)
    private String endCell = "[109873,53574]";

    public static void main(String... argv) {
        Test main = new Test();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        try {
            main.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void run() throws IOException {

//        Cell startCell = new Cell("[2,1]"), endCell = new Cell("[1,32]");
//        System.out.println(startCell.hashCode() + " " + endCell.hashCode());
//        Plot plt = Plot.create(PythonConfig.pythonBinPathConfig("python3"));
        List<String> lines = FileUtils.readLines(FileUtils.getFile("hotsd.txt"), "UTF-8");

        List<sdinfo> output = Collections.synchronizedList(new ArrayList<>());
        lines.parallelStream().forEach(line -> {
            String[] splits = line.split("[ \t]");
            Cell startCell = new Cell(splits[0]), endCell = new Cell(splits[1]);

            // Origin trajectory.
            Map<String, List<GPS>> trajectoryGPS = TrajectoryUtil.getAllTrajectoryGPSs(startCell, endCell, city);
            System.out.println("trajectoryGPS size: " + trajectoryGPS.size());
            // Trajectory info.
            Map<String, double[]> trajectoryInfoMap = Maps.transformValues(trajectoryGPS, s -> CommonUtil.trajectoryInfo(s, city));

            // Generate training set.
            double[] distArray = trajectoryInfoMap.values().stream().mapToDouble(s -> s[0]).toArray();
            double[] timeArray = trajectoryInfoMap.values().stream().mapToDouble(s -> s[1]).toArray();

            double distance60 = StatUtils.percentile(distArray, 60);
            double time60 = StatUtils.percentile(timeArray, 60);
//        double distance95 = CommonUtil.percentile(distArray, 0.90);
//        double time95 = CommonUtil.percentile(timeArray, 0.90);
            if (Double.isNaN(distance60) || Double.isNaN(time60)) {
                return;
            }
            Map<String, List<GPS>> trainTrajectory = Maps.filterKeys(trajectoryGPS, Maps.filterValues(trajectoryInfoMap, s -> s[0] < distance60 || s[1] < time60)::containsKey);
            System.out.println("Train set size:" + trainTrajectory.size());

            List<Double>[] timeList = new List[24];
//        List<Double>[] distanceList = new List[24];

            for (Map.Entry<String, List<GPS>> entry : trainTrajectory.entrySet()) {
                int hour = entry.getValue().get(0).getTimestamp().getHours();
                if (timeList[hour] == null) {
                    timeList[hour] = new ArrayList<>();
//                distanceList[hour] = new ArrayList<>();
                }
                double[] info = trajectoryInfoMap.get(entry.getKey());//dist time
//            distanceList[hour].add(info[0]);
                timeList[hour].add(info[1]);
            }

            //transform to mean
            double[] meanTime = Arrays.stream(timeList).mapToDouble(this::missMean).toArray();
            double[] stdMeanTime = missNormalize(meanTime);

//            plt.plot().add(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23), Arrays.stream(stdMeanTime).boxed().collect(Collectors.toList()));
            //        double[] meanDistance = Arrays.stream(distanceList).mapToDouble(x -> StatUtils.mean(x.stream().mapToDouble(y -> y).toArray())).toArray();
            System.out.println(startCell + " " + endCell);
//            System.out.println(Arrays.toString(meanTime));
            System.out.println(Arrays.toString(stdMeanTime));
            output.add(new sdinfo(trainTrajectory.size(), TrajectoryUtil.numMap.get(startCell), TrajectoryUtil.numMap.get(endCell), startCell, endCell, TileSystem.TileToGPS(startCell), TileSystem.TileToGPS(endCell), stdMeanTime));

        });

        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.serializeSpecialFloatingPointValues();
        Gson gson = gsonBuilder.create();
        try {
            FileUtils.write(FileUtils.getFile("sd_json.json"), gson.toJson(output), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

//        try {
//            plt.show();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (PythonExecutionException e) {
//            e.printStackTrace();
//        }
    }

    private double missMean(List<Double> list) {
        if (list == null) {
            return -1;
        }
        return StatUtils.mean(list.stream().mapToDouble(y -> y).toArray());
    }

    private double[] missNormalize(final double[] nums) {
        double sum = 0, n = 0;
        for (double num : nums) {
            if (num != -1) {
                sum += num;
                n++;
            }
        }
        double mean = sum / n, accu = 0, dev = 0;
        for (double num : nums) {
            if (num != -1) {
                dev = num - mean;
                accu += dev * dev;
            }
        }
        double std = Math.sqrt(accu / (n - 1));
        double[] result = new double[nums.length];
        for (int i = 0; i < nums.length; i++) {
            if (nums[i] != -1) {
                result[i] = (nums[i] - mean) / std;
            }
        }
        return result;
    }

    class sdinfo implements Serializable {
        int startNum, endNum, totalNum;
        Cell startCell, endCell;
        GPS startGPS, endGPS;
        double[] dist;

        sdinfo(int totalNum, int startNum, int endNum, Cell startCell, Cell endCell, GPS startGPS, GPS endGPS, double[] dist) {
            this.totalNum = totalNum;
            this.startNum = startNum;
            this.endNum = endNum;
            this.startCell = new Cell(startCell);
            this.endCell = new Cell(endCell);
            this.startGPS = new GPS(startGPS);
            this.endGPS = new GPS(endGPS);
            this.dist = Arrays.copyOf(dist, dist.length);
        }
    }

}
