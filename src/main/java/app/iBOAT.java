package app;

import algorithm.iBOATDetection;
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
 * Detect all trajectories by iBOAT algorithm.
 *
 * @author Bin Cheng
 */
public class iBOAT {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = true, validateWith = CommonUtil.CityValidator.class)
    private String city;
    @Parameter(names = {"--debug", "-d"}, description = "Debug mode.")
    private Boolean debug = false;
    @Parameter(names = {"-t"}, description = "Threshold.")
    private double threshold = 0.04;

    @Parameter(names = {"-s"}, description = "Start cell.", validateWith = CommonUtil.CellValidator.class)
    private String startCell = "[109776,53554]";
    @Parameter(names = {"-e"}, description = "End cell.", validateWith = CommonUtil.CellValidator.class)
    private String endCell = "[109873,53574]";

    public static void main(String... argv) {
        iBOAT main = new iBOAT();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        main.run();
    }

    private void run() {

        Cell startCell = new Cell(this.startCell);
        Cell endCell = new Cell(this.endCell);

        // GPS trajectory.
        Map<String, List<GPS>> trajectoryGPS = TrajectoryUtil.getAllTrajectoryGPSs(startCell, endCell, city);
        System.out.println("trajectoryGPS size: " + trajectoryGPS.size());
        // Cell trajectory.
        Map<String, List<Cell>> trajectoryCell = Maps.filterKeys(TrajectoryUtil.getAllTrajectoryCells(startCell, endCell, city), trajectoryGPS::containsKey);
        System.out.println("trajectoryCell size: " + trajectoryCell.size());

        // Anomaly Label.
        Set<String> anomalyTrajectory = CommonUtil.anomalyTrajectory(trajectoryGPS, city, debug);

        long start = System.currentTimeMillis();
        // Detection.
        Set<String> iBOATAnomaly = trajectoryGPS.entrySet().parallelStream()
                .filter(entry -> {
                            Map<String, List<Cell>> tmp = new HashMap<>(trajectoryCell);
                            tmp.remove(entry.getKey());
                            double score = iBOATDetection.iBOAT(entry.getValue(), new ArrayList<>(tmp.values()), threshold);
                            return score > 0.1;
                        }
                ).map(Map.Entry::getKey).collect(Collectors.toSet());

        long end = System.currentTimeMillis();

        // Evaluation.
        int tp = Sets.intersection(anomalyTrajectory, iBOATAnomaly).size();
        int fp = Sets.intersection(Sets.difference(trajectoryGPS.keySet(), anomalyTrajectory), iBOATAnomaly).size();
        int fn = anomalyTrajectory.size() - tp;
        int tn = trajectoryGPS.size() - anomalyTrajectory.size() - fp;

        CommonUtil.printResult(tp, fp, fn, tn);

        System.out.println("Pre Time: " + (end - start) * 1.0 / trajectoryGPS.size() / 1000);

    }

}
