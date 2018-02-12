package app;

import algorithm.iBATDetection;
import app.bean.TrajectoryInfo;
import bean.Cell;
import bean.GPS;
import com.google.gson.Gson;
import constant.DetectConstant;
import hbase.TrajectoryUtil;
import org.apache.commons.io.FileUtils;
import util.CommonUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Detect all trajectories by iBAT algorithm.
 *
 * @author Bin Cheng
 */
public class iBAT {

    public static void main(String[] args) throws IOException {

        Cell startPoint = DetectConstant.startPoint;
        Cell endPoint = DetectConstant.endPoint;
        int numOfTrial = 50;
        int subSampleSize = 200;

        if (args.length == 2) {
            numOfTrial = Integer.parseInt(args[0]);
            subSampleSize = Integer.parseInt(args[1]);
        }


        Map<String, List<Cell>> allTrajectories = TrajectoryUtil.getAllTrajectoryCells(startPoint, endPoint);

        System.out.println("numOfTrial:" + numOfTrial);
        System.out.println("subSampleSize:" + subSampleSize);
        System.out.println(allTrajectories.size());

        List<TrajectoryInfo> trajectoryInfos = new ArrayList<>();
        Set<String> iBATSetN = new HashSet<>();
        Set<String> iBATSetA = new HashSet<>();

        long total = 0;
        for (String trajectoryID : allTrajectories.keySet()) {
            Map<String, List<Cell>> tmp = new HashMap<>(allTrajectories);
            tmp.remove(trajectoryID);
            List<Cell> testTrajectory = CommonUtil.removeExtraCell(TrajectoryUtil.getTrajectoryCells(trajectoryID), startPoint, endPoint);
            if (testTrajectory == null || testTrajectory.size() == 0)
                continue;
            List<GPS> testTrajectoryGPS = TrajectoryUtil.getTrajectoryGPSPoints(trajectoryID);
            testTrajectoryGPS = CommonUtil.removeExtraGPS(testTrajectoryGPS, startPoint, endPoint);
            if (testTrajectoryGPS == null || testTrajectoryGPS.size() == 0)
                continue;

            long start = System.currentTimeMillis();
            double score = iBATDetection.iBAT(testTrajectory, new ArrayList<>(tmp.values()), numOfTrial, subSampleSize);
            long end = System.currentTimeMillis();
            total += end - start;

            TrajectoryInfo trajectoryInfo = new TrajectoryInfo();
            trajectoryInfo.taxiId = trajectoryID;
            trajectoryInfo.score = score;
            trajectoryInfo.normal = score < 0.65;
            if (trajectoryInfo.normal)
                iBATSetN.add(trajectoryID);
            else
                iBATSetA.add(trajectoryID);

            trajectoryInfo.trajectory = testTrajectoryGPS;

            trajectoryInfos.add(trajectoryInfo);
        }

        System.out.println(total);


        trajectoryInfos.sort((o1, o2) -> Double.compare(o2.score, o1.score));
        List<String> anomaly = FileUtils.readLines(FileUtils.getFile("iBAT.txt"));

        int rankTotal = 0;
        for (int i = trajectoryInfos.size(); i > 0; i--) {
            if (anomaly.contains(trajectoryInfos.get(i - 1).taxiId)) {
                rankTotal += i;
            }
        }
        int M = anomaly.size();
        int N = trajectoryInfos.size() - anomaly.size();
        double auc = (rankTotal - (M + 1) * M * 1.0 / 2) * 1.0 / (M * N);
        System.out.println("auc:" + auc);

        Set<String> preAnomaly = new HashSet<>(iBATSetA);
        Set<String> preNormal = new HashSet<>(iBATSetN);
        int allAnomaly = preAnomaly.size();
        preAnomaly.retainAll(anomaly);
        System.out.println("TP:" + preAnomaly.size());
        System.out.println("FP:" + (allAnomaly - preAnomaly.size()));

        int allNormal = preNormal.size();
        preNormal.retainAll(anomaly);
        System.out.println("FN:" + preNormal.size());
        System.out.println("TN:" + (allNormal - preNormal.size()));

    }


}
