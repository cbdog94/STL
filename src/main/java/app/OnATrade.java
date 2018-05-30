package app;

import algorithm.OnATradeDetection;
import bean.Cell;
import bean.GPS;
import bean.Section;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import hbase.TrajectoryUtil;
import util.CommonUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OnATrade {

    @Parameter(names = {"--city", "-c"}, description = "Which city to be counted (SH/SZ/CD).", required = false, validateWith = CommonUtil.CityValidator.class)
    private String city = "SH";
    @Parameter(names = {"--debug", "-d"}, description = "Debug mode.")
    private Boolean debug = false;
    @Parameter(names = {"-t"}, description = "The threshold of anomaly.")
    private double threshold = 0.13127;

    @Parameter(names = {"-s"}, description = "Start cell.", validateWith = CommonUtil.CellValidator.class)
    private String startCell = "[109776,53554]";
    @Parameter(names = {"-e"}, description = "End cell.", validateWith = CommonUtil.CellValidator.class)
    private String endCell = "[109873,53574]";


    public static void main(String... argv) {
        OnATrade main = new OnATrade();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        main.run();
    }

    private void run() {

        Cell startCell = new Cell(this.startCell);
        Cell endCell = new Cell(this.endCell);

        OnATradeDetection onATradeDetection = new OnATradeDetection(debug);
        // Origin trajectory.
        Map<String, List<GPS>> trajectoryGPS = TrajectoryUtil.getAllTrajectoryGPSs(startCell, endCell, city);
        System.out.println("trajectoryGPS size: " + trajectoryGPS.size());
        // Generate abstract trajectory.
        Map<String, List<Section>> absTrajectory = onATradeDetection.generateAbstractTrajectory(startCell, endCell, trajectoryGPS, city);
        System.out.println("Abstract Trajectory Size: " + absTrajectory.size());
        // Generate recommend trajectory.
        Map<String, List<Section>> recommendTrajectory = onATradeDetection.routeRecommend(new HashMap<>(absTrajectory), 7, 0.8742, 0.6173);
        System.out.println("Recommend Trajectory Size: " + recommendTrajectory.size());

        trajectoryGPS = Maps.filterKeys(trajectoryGPS, absTrajectory::containsKey);
        // Label.
        Set<String> anomalyTrajectory = CommonUtil.anomalyTrajectory(trajectoryGPS, city, debug);

//        05bbf71fdaf54c3e81c76e548f460f58

        long start = System.currentTimeMillis();
        // Detection.
        Set<String> onATradeTrajectory = absTrajectory.entrySet().parallelStream()
                .filter(s -> onATradeDetection.detection(s.getValue(), recommendTrajectory, threshold) > 0.1)
                .map(Map.Entry::getKey).collect(Collectors.toSet());
        long end = System.currentTimeMillis();

        // Evaluation.
        int TP = Sets.intersection(anomalyTrajectory, onATradeTrajectory).size();
        int FP = Sets.intersection(Sets.difference(trajectoryGPS.keySet(), anomalyTrajectory), onATradeTrajectory).size();
        int FN = anomalyTrajectory.size() - TP;
        int TN = trajectoryGPS.size() - anomalyTrajectory.size() - FP;

        CommonUtil.printResult(TP, FP, FN, TN);

        System.out.println("Pre Time: " + (end - start) * 1.0 / trajectoryGPS.size() / 1000);

    }


}
