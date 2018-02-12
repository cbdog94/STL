package app;

import algorithm.STLDetection;
import bean.Cell;
import bean.GPS;
import constant.DetectConstant;
import hbase.TrajectoryUtil;
import util.CommonUtil;

import java.util.*;

/**
 * Spatial-Temporal Laws
 */
public class STL {


    public static void main(String[] args) {

        Cell startPoint = DetectConstant.startPoint;
        Cell endPoint = DetectConstant.endPoint;
        double thresholdTime = 0.7;
        double thresholdDist = 0.7;
        int degree = 3;
        double quantile = 0.6;

        if (args.length == 4) {
            degree = Integer.parseInt(args[0]);
            thresholdTime = Double.parseDouble(args[1]);
            thresholdDist = Double.parseDouble(args[2]);
            quantile = Double.parseDouble(args[3]);
        }

        if (args.length == 5) {
            degree = Integer.parseInt(args[0]);
            thresholdTime = Double.parseDouble(args[1]);
            thresholdDist = Double.parseDouble(args[2]);
            quantile = Double.parseDouble(args[3]);
            Cell[] cells = DetectConstant.getCellPoint(Integer.parseInt(args[4]));
            startPoint = cells[0];//陆家嘴
            endPoint = cells[1];//浦东机场
        }

        System.out.println("degree " + degree);
        System.out.println("thresholdT " + thresholdTime);
        System.out.println("thresholdD " + thresholdDist);
        System.out.println("quantile " + quantile);

        Set<String> IDS = TrajectoryUtil.getAllTrajectoryID(startPoint, endPoint);


        Map<String, List<GPS>> gpsTrajectories = new HashMap<>();
        Map<String, Double> distance = new HashMap<>();
        Map<String, Double> time = new HashMap<>();//min

        for (String id : IDS) {
            List<GPS> testTrajectory = TrajectoryUtil.getTrajectoryGPSPoints(id);
            testTrajectory = CommonUtil.removeExtraGPS(testTrajectory, startPoint, endPoint);
            if (testTrajectory == null || testTrajectory.size() == 0) {
                continue;
            }
            distance.put(id, CommonUtil.trajectoryDistance(testTrajectory));
            long endTime = testTrajectory.get(testTrajectory.size() - 1).getTimestamp().getTime();
            long startTime = testTrajectory.get(0).getTimestamp().getTime();

            time.put(id, (endTime - startTime) * 1.0 / (60 * 1000));
            gpsTrajectories.put(id, testTrajectory);
        }

        System.out.println("all size:" + gpsTrajectories.size());


        double distance90 = CommonUtil.percentile(distance.values().toArray(new Double[distance.size()]), 0.85);
        double time90 = CommonUtil.percentile(time.values().toArray(new Double[time.size()]), 0.85);

        double distance60 = CommonUtil.percentile(distance.values().toArray(new Double[distance.size()]), quantile);
        double time60 = CommonUtil.percentile(time.values().toArray(new Double[time.size()]), quantile);

        List<String> anomaly = new ArrayList<>();
        for (String id : distance.keySet()) {
            if (distance.get(id) > distance90 && time.get(id) > time90)
                anomaly.add(id);
        }

        //train set id
        Map<String, List<GPS>> allList = new HashMap<>();
        for (String id : distance.keySet()) {
            if (distance.get(id) < distance60 || time.get(id) < time60) {
                allList.put(id, gpsTrajectories.get(id));
            }
        }

        System.out.println("train set size:" + allList.size());
        System.out.println("distance set size:" + distance.size());
        System.out.println("time set size:" + time.size());

        long totalDTM = 0;
        //Only DTM
        Set<String> DTMAnomaly = new HashSet<>();
        Set<String> DTMNormal = new HashSet<>();
        for (String trajectoryID : gpsTrajectories.keySet()) {
            Map<String, List<GPS>> tmp = new HashMap<>(allList);
            tmp.remove(trajectoryID);

            long start = System.currentTimeMillis();
            double score = STLDetection.detect(new ArrayList<>(tmp.values()), gpsTrajectories.get(trajectoryID), thresholdTime, thresholdDist, degree);
            long end = System.currentTimeMillis();
            totalDTM += end - start;

            if (score < 0.1)
                DTMNormal.add(trajectoryID);
            else
                DTMAnomaly.add(trajectoryID);
        }

        int allAnomaly = DTMAnomaly.size();
        int allNormal = DTMNormal.size();

        DTMAnomaly.retainAll(anomaly);
        DTMNormal.retainAll(anomaly);
//        System.out.println(allAnomaly);
//        System.out.println(allNormal);
        int TP = DTMAnomaly.size();
        int FP = allAnomaly - DTMAnomaly.size();
        int FN = DTMNormal.size();
        int TN = allNormal - DTMNormal.size();

        System.out.println("-----------");
        System.out.println("TP:" + TP);
        System.out.println("FP:" + FP);
        System.out.println("FN:" + FN);
        System.out.println("TN:" + TN);

        System.out.println("TPR:" + TP * 1.0 / (TP + FN));
        System.out.println("FPR:" + FP * 1.0 / (FP + TN));

//        System.out.println("Total Time: " + totalDTM);
        System.out.println("Pre Time: " + totalDTM * 1.0 / gpsTrajectories.size() / 1000);
    }

}
