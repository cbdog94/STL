package app;

import algorithm.STLDetection;
import bean.Cell;
import bean.GPS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Maps;
import hbase.TrajectoryUtil;
import org.apache.commons.math3.stat.StatUtils;
import util.CommonUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spatial-Temporal Laws
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

//        Cell startCell = new Cell(this.startCell);
//        Cell endCell = new Cell(this.endCell);

        Cell startCell = new Cell("[109776,53554]");
        Cell endCell = new Cell("[109802,53581]");

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

        Map<String, List<GPS>> trainTrajectory = Maps.filterKeys(trajectoryGPS, Maps.filterValues(trajectoryInfoMap, s -> s[0] < distance60 || s[1] < time60)::containsKey);
        System.out.println("Train set size:" + trainTrajectory.size());

        List<GPS> test = trainTrajectory.remove("ef169fae9f6f40ed845e691b4a9666e2");
        STLDetection.detect(new ArrayList<>(trainTrajectory.values()), test, thresholdTime, thresholdDist, degree);


        // Anomaly Label.
//        Set<String> anomalyTrajectory = CommonUtil.anomalyTrajectory(trajectoryGPS, city, debug);
//
//        // Detection
//        long start = System.currentTimeMillis();
//
//        Set<String> STLAnomaly = trajectoryGPS.entrySet().parallelStream()
//                .filter(entry -> {
//                            Map<String, List<GPS>> tmp = new HashMap<>(trajectoryGPS);
//                            tmp.remove(entry.getKey());
//                            double score = STLDetection.detect(new ArrayList<>(tmp.values()), entry.getValue(), thresholdTime, thresholdDist, degree);
//                            return score > 0.1;
//                        }
//                ).map(Map.Entry::getKey).collect(Collectors.toSet());
//
//        long end = System.currentTimeMillis();
//
//        // Evaluation.
//        int TP = Sets.intersection(anomalyTrajectory, STLAnomaly).size();
//        int FP = Sets.intersection(Sets.difference(trajectoryGPS.keySet(), anomalyTrajectory), STLAnomaly).size();
//        int FN = anomalyTrajectory.size() - TP;
//        int TN = trajectoryGPS.size() - anomalyTrajectory.size() - FP;
//
//        CommonUtil.printResult(TP, FP, FN, TN);
//
//        System.out.println("Pre Time: " + (end - start) * 1.0 / trajectoryGPS.size() / 1000);
    }

}
