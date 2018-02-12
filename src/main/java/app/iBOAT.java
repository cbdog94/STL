package app;

import algorithm.iBOATDetection;
import app.bean.TrajectoryInfo;
import bean.Cell;
import bean.GPS;
import constant.DetectConstant;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;
import util.CommonUtil;
import util.TileSystem;

import java.io.IOException;
import java.util.*;

/**
 * Detect all trajectories by iBOAT algorithm.
 *
 * @author Bin Cheng
 */
public class iBOAT {

    public static void main(String[] args) throws IOException {

        Cell startPoint = DetectConstant.startPoint;
        Cell endPoint = DetectConstant.endPoint;
        String fileName = "";
        double threshold = iBOATDetection.threshold;

        if (args.length == 4) {
            String[] start = args[0].split(",");
            String[] end = args[1].split(",");
            startPoint = TileSystem.GPSToTile(new GPS(Double.parseDouble(start[0]), Double.parseDouble(start[1]), null));
            endPoint = TileSystem.GPSToTile(new GPS(Double.parseDouble(end[0]), Double.parseDouble(end[1]), null));
            fileName = args[2];
            threshold = Double.parseDouble(args[3]);
        } else {
            System.out.println("please input startLat,startLng endLat,endLng anomalyFile threshold");
            return;
        }


        Map<String, List<Cell>> allTrajectories = TrajectoryUtil.getAllTrajectoryCells(startPoint, endPoint);

        System.out.println("threshold:" + threshold);
        System.out.println("all size:" + allTrajectories.size());
        Set<String> iBOATSetN = new HashSet<>();
        Set<String> iBOATSetA = new HashSet<>();

        List<TrajectoryInfo> trajectoryInfos = new ArrayList<>();
        long total = 0;
        for (String trajectoryID : allTrajectories.keySet()) {

            List<GPS> testTrajectory = TrajectoryUtil.getTrajectoryGPSPoints(trajectoryID);
            testTrajectory = CommonUtil.removeExtraGPS(testTrajectory, startPoint, endPoint);
            if (testTrajectory == null || testTrajectory.size() == 0) {
                continue;
            }
            Map<String, List<Cell>> tmpTrajectories = new HashMap<>(allTrajectories);
            tmpTrajectories.remove(trajectoryID);

            long start = System.currentTimeMillis();
            double score = iBOATDetection.iBOAT(testTrajectory, new ArrayList<>(tmpTrajectories.values()), threshold);
            long end = System.currentTimeMillis();
            total += end - start;

            TrajectoryInfo trajectoryInfo = new TrajectoryInfo();
            trajectoryInfo.taxiId = trajectoryID;
            trajectoryInfo.score = score;
            trajectoryInfo.normal = score < 0.1;

            if (trajectoryInfo.normal)
                iBOATSetN.add(trajectoryID);
            else
                iBOATSetA.add(trajectoryID);

            trajectoryInfo.trajectory = testTrajectory;

            trajectoryInfos.add(trajectoryInfo);

        }

        System.out.println(total);

        trajectoryInfos.sort((o1, o2) -> Double.compare(o2.score, o1.score));
        List<String> anomaly = FileUtils.readLines(FileUtils.getFile(fileName));

//        System.out.println(trajectoryInfos.get(0).score);
        int rankTotal = 0;
        for (int i = trajectoryInfos.size(); i > 0; i--) {
            if (anomaly.contains(trajectoryInfos.get(i - 1).taxiId)) {
                rankTotal += i;
//                System.out.println(i);
            }
        }
//        System.out.println(rankTotal);
        int M = anomaly.size();
        int N = trajectoryInfos.size() - anomaly.size();
        double auc = (rankTotal - (M + 1) * M * 1.0 / 2) * 1.0 / (M * N);
        System.out.println("auc:" + auc);

        Set<String> preAnomaly = new HashSet<>(iBOATSetA);
        Set<String> preNormal = new HashSet<>(iBOATSetN);
        int allAnomaly = preAnomaly.size();
        preAnomaly.retainAll(anomaly);
        System.out.println("TP:" + preAnomaly.size());
        System.out.println("FP:" + (allAnomaly - preAnomaly.size()));

        int allNormal = preNormal.size();
        preNormal.retainAll(anomaly);
        System.out.println("FN:" + preNormal.size());
        System.out.println("TN:" + (allNormal - preNormal.size()));

        int TP = preAnomaly.size();
        int FP = allAnomaly - preAnomaly.size();
        int FN = preNormal.size();
        int TN = allNormal - preNormal.size();

        System.out.println("TPR:" + TP * 1.0 / (TP + FN));
        System.out.println("FPR:" + FP * 1.0 / (FP + TN));


    }

}
