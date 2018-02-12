package app;

import algorithm.STLDetection;
import algorithm.iBOATDetection;
import bean.Cell;
import bean.GPS;
import constant.DetectConstant;
import hbase.TrajectoryUtil;
import util.CommonUtil;

import java.util.*;

/**
 * iBOAT and STL
 */
public class iBOATAndSTL {


    public static void main(String[] args) {

        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("win")) {
            System.out.println(os);
            System.setProperty("hadoop.home.dir", "D:\\hadoop");
        }

        Cell startPoint = DetectConstant.startPoint;
        Cell endPoint = DetectConstant.endPoint;
        double thresholdTime = 0.8;
        double thresholdDist = 0.8;
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
        System.out.println("threshold Time" + thresholdTime);
        System.out.println("threshold Distance" + thresholdDist);
        System.out.println("quantile " + quantile);

        long start = System.currentTimeMillis();
        Map<String, List<Cell>> allTrajectories = TrajectoryUtil.getAllTrajectoryCells(startPoint, endPoint);
        long end = System.currentTimeMillis();
        long extract = end - start;

        Set<String> iBOATSetN = new HashSet<>();
        Set<String> iBOATSetA = new HashSet<>();

        Map<String, List<GPS>> gpsTrajectories = new HashMap<>();
        Map<String, Double> distance = new HashMap<>();
        Map<String, Double> time = new HashMap<>();//min

        for (String id : new HashSet<>(allTrajectories.keySet())) {
            List<GPS> testTrajectory = TrajectoryUtil.getTrajectoryGPSPoints(id);
            testTrajectory = CommonUtil.removeExtraGPS(testTrajectory, startPoint, endPoint);
            if (testTrajectory == null || testTrajectory.size() == 0) {
                allTrajectories.remove(id);
                continue;
            }
            distance.put(id, CommonUtil.trajectoryDistance(testTrajectory));
            long endTime = testTrajectory.get(testTrajectory.size() - 1).getTimestamp().getTime();
            long startTime = testTrajectory.get(0).getTimestamp().getTime();

            time.put(id, (endTime - startTime) * 1.0 / (60 * 1000));
            gpsTrajectories.put(id, testTrajectory);
        }


        System.out.println("all size:" + gpsTrajectories.size());


        long totalIBOAT = 0;

        for (String id : gpsTrajectories.keySet()) {

            Map<String, List<Cell>> tmpTrajectories = new HashMap<>(allTrajectories);
            tmpTrajectories.remove(id);

            start = System.currentTimeMillis();
            double result = iBOATDetection.iBOAT(gpsTrajectories.get(id), new ArrayList<>(tmpTrajectories.values()));
            end = System.currentTimeMillis();

            totalIBOAT += end - start;

            if (result < 0.1)
                iBOATSetN.add(id);
            else
                iBOATSetA.add(id);
        }

        System.out.println("TotalTime:" + totalIBOAT);
        double perTime = totalIBOAT * 1.0 / (iBOATSetN.size() + iBOATSetA.size());
        System.out.println("PerTime:" + (perTime));
        System.out.println("finish iBOAT!");

        System.out.println("iBOAT Normal size:" + iBOATSetN.size());
        System.out.println("iBOAT Anomaly size:" + iBOATSetA.size());

        double distance90 = CommonUtil.percentile(distance.values().toArray(new Double[distance.size()]), 0.85);
        double time90 = CommonUtil.percentile(time.values().toArray(new Double[time.size()]), 0.85);

        double distance60 = CommonUtil.percentile(distance.values().toArray(new Double[distance.size()]), quantile);
        double time60 = CommonUtil.percentile(time.values().toArray(new Double[time.size()]), quantile);

        List<String> anomaly = new ArrayList<>();
        for (String id : distance.keySet()) {
            if (distance.get(id) > distance90 && time.get(id) > time90)
                anomaly.add(id);
        }

        List<List<GPS>> trainList = new ArrayList<>();
        for (String id : distance.keySet()) {
            if (distance.get(id) < distance60 || time.get(id) < time60)
                trainList.add(gpsTrajectories.get(id));
        }
        System.out.println("train set size:" + trainList.size());

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

        System.out.println("---------------");

        //Only STL
        Set<String> DTMAnomaly = new HashSet<>();
        Set<String> DTMNormal = new HashSet<>();

        long totalDTM = 0;
        for (String trajectoryID : gpsTrajectories.keySet()) {

            start = System.currentTimeMillis();
            double score = STLDetection.detect(trainList, gpsTrajectories.get(trajectoryID), thresholdTime, thresholdDist, degree);
            end = System.currentTimeMillis();
            totalDTM += end - start;

            if (score < 0.1)
                DTMNormal.add(trajectoryID);
            else
                DTMAnomaly.add(trajectoryID);
        }

        allAnomaly = DTMAnomaly.size();
        allNormal = DTMNormal.size();
        Set<String> tmpDTMAnomaly = new HashSet<>(DTMAnomaly);
        Set<String> tmpDTMNormal = new HashSet<>(DTMNormal);
        tmpDTMAnomaly.retainAll(anomaly);
        tmpDTMNormal.retainAll(anomaly);
        TP = tmpDTMAnomaly.size();
        FP = allAnomaly - tmpDTMAnomaly.size();
        FN = tmpDTMNormal.size();
        TN = allNormal - tmpDTMNormal.size();
        System.out.println("DTM-----------");
        System.out.println("TotalTime:" + totalDTM);
        perTime = totalDTM * 1.0 / gpsTrajectories.size();
        System.out.println("PerTime:" + perTime);
        System.out.println("TP:" + TP);
        System.out.println("FP:" + FP);
        System.out.println("FN:" + FN);
        System.out.println("TN:" + TN);

        System.out.println("TPR:" + TP * 1.0 / (TP + FN));
        System.out.println("FPR:" + FP * 1.0 / (FP + TN));
        System.out.println("---------------");

    }


}
