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
 */
public class TimeSlot {

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
        TimeSlot main = new TimeSlot();
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
        Set<String> anomalyTrajectory = new HashSet<>(CommonUtil.anomalyTrajectory(trajectoryGPS, city, debug));
//        System.out.println(anomalyTrajectory);

        // Detection
        long start = System.currentTimeMillis();

        Set<String> STLAnomaly =  new HashSet<>(trajectoryGPS.entrySet().parallelStream()
                .filter(entry -> {
                            Map<String, List<GPS>> tmp = new HashMap<>(trajectoryGPS);
                            tmp.remove(entry.getKey());
                            double score = STLDetection.detect(new ArrayList<>(tmp.values()), entry.getValue(), thresholdTime, thresholdDist, degree);
                            return score > 0.1;
                        }
                ).map(Map.Entry::getKey).collect(Collectors.toSet()));
//        System.out.println(STLAnomaly.size());
        long end = System.currentTimeMillis();

        List<String>[] timeMap = new List[24];
        for (Map.Entry<String, List<GPS>> entry : trajectoryGPS.entrySet()) {
            int hour = entry.getValue().get(0).getTimestamp().getHours();
            if (timeMap[hour] == null) {
                timeMap[hour] = new ArrayList<>();
            }
            timeMap[hour].add(entry.getKey());
        }

        int index = 0;
        for (List<String> timeSlot : timeMap) {
            if (timeSlot != null) {
                int tp = 0, fp = 0, tn = 0, fn = 0;
                for (String id : timeSlot) {
//                    System.out.println(id);
                    boolean realAnomaly = anomalyTrajectory.contains(id);
                    boolean stlAnomaly = STLAnomaly.contains(id);
                    if (realAnomaly && stlAnomaly) {
                        tp++;
                    } else if (realAnomaly && !stlAnomaly) {
                        fn++;
                    } else if (!realAnomaly && stlAnomaly) {
                        fp++;
                    } else {
                        tn++;
                    }
                }
//                System.out.println(timeSlot.size());
//                System.out.println(tp+" "+fn+" "+fp+" "+tn);
                System.out.println(index + "-TPR: " + tp * 1.0 / (tp + fn));
                System.out.println(index + "-FPR: " + fp * 1.0 / (fp + tn));
            }
            index++;
        }

        // Evaluation.
        int TP = Sets.intersection(anomalyTrajectory, STLAnomaly).size();
        int FP = Sets.intersection(Sets.difference(trajectoryGPS.keySet(), anomalyTrajectory), STLAnomaly).size();
        int FN = anomalyTrajectory.size() - TP;
        int TN = trajectoryGPS.size() - anomalyTrajectory.size() - FP;

        CommonUtil.printResult(TP, FP, FN, TN);

        System.out.println("Pre Time: " + (end - start) * 1.0 / trajectoryGPS.size() / 1000);
    }

}
