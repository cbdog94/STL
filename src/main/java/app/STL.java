package app;

import algorithm.STLDetection;
import bean.Cell;
import bean.GPS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import hbase.TrajectoryUtil;
import util.CommonUtil;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Spatial-Temporal Laws
 *
 * @author Bin Cheng
 */
public class STL {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;
    @Parameter(names = {"--debug", "-d"}, description = "Debug mode.")
    private Boolean debug = false;
    @Parameter(names = {"--degree", "-deg"}, description = "Model complexity.")
    private int degree = 3;
    @Parameter(names = {"-tT"}, description = "Threshold of time.")
    private double thresholdTime = 0.7;
    @Parameter(names = {"-tD"}, description = "Threshold of distance.")
    private double thresholdDist = 0.7;

    @Parameter(names = {"-s"}, description = "Start cell.", validateWith = CommonUtil.CellValidator.class)
    private String startCell = "[109776,53554]";
    @Parameter(names = {"-e"}, description = "End cell.", validateWith = CommonUtil.CellValidator.class)
    private String endCell = "[109873,53574]";

    public static void main(String... argv) {
        STL main = new STL();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        main.run();
    }

    private void run() {

        Cell startCell = new Cell(this.startCell);
        Cell endCell = new Cell(this.endCell);

        // Origin trajectory.
        Map<String, List<GPS>> trajectoryGPS = TrajectoryUtil.getAllTrajectoryGPSs(startCell, endCell, city);
        System.out.println("trajectoryGPS size: " + trajectoryGPS.size());

        // Trajectory info.
        Map<String, double[]> trajectoryInfoMap = Maps.transformValues(trajectoryGPS, s -> CommonUtil.trajectoryInfo(s, city));

        // Generate training set.
        double[] distArray = trajectoryInfoMap.values().stream().mapToDouble(s -> s[0]).toArray();
        double[] timeArray = trajectoryInfoMap.values().stream().mapToDouble(s -> s[1]).toArray();

        double distance60 = CommonUtil.percentile(distArray, 0.6);
        double time60 = CommonUtil.percentile(timeArray, 0.6);

        Map<String, List<GPS>> trainTrajectory = Maps.filterKeys(trajectoryGPS, Maps.filterValues(trajectoryInfoMap, s -> s[0] < distance60 || s[1] < time60)::containsKey);
        System.out.println("Train set size:" + trainTrajectory.size());

        // Anomaly Label.
        Set<String> anomalyTrajectory = CommonUtil.anomalyTrajectory(trajectoryGPS, city, debug);

        // Detection
        long start = System.currentTimeMillis();

        Set<String> STLAnomaly = trajectoryGPS.entrySet().parallelStream()
                .filter(entry -> {
                            Map<String, List<GPS>> tmp = new HashMap<>(trajectoryGPS);
                            tmp.remove(entry.getKey());
                            double score = STLDetection.detect(new ArrayList<>(tmp.values()), entry.getValue(), thresholdTime, thresholdDist, degree);
                            return score > 0.1;
                        }
                ).map(Map.Entry::getKey).collect(Collectors.toSet());

        long end = System.currentTimeMillis();

        // Evaluation.
        int tp = Sets.intersection(anomalyTrajectory, STLAnomaly).size();
        int fp = Sets.intersection(Sets.difference(trajectoryGPS.keySet(), anomalyTrajectory), STLAnomaly).size();
        int fn = anomalyTrajectory.size() - tp;
        int tn = trajectoryGPS.size() - anomalyTrajectory.size() - fp;

        CommonUtil.printResult(tp, fp, fn, tn);

        System.out.println("Pre Time: " + (end - start) * 1.0 / trajectoryGPS.size() / 1000);
    }

}
